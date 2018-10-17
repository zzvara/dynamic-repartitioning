package hu.sztaki.drc

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import hu.sztaki.drc.partitioner._
import hu.sztaki.drc.utilities.{Configuration, Exception, Logger}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * A decider strategy, that continuously receives histograms from the physical tasks
  * and decides when and how to repartition a certain stage of computation.
  *
  * That means a decider strategy is always bound to a specific stage in batch mode, or
  * to a stream in streaming mode. A `stageID` of Spark should be a `vertexID` of Flink.
  *
  * @todo Add support to check distance from uniform distribution.
  * @param stageID
  * @param resourceStateHandler
  */
abstract class Decider(
  /**
   * A Spark stage ID, stream ID or a vertex ID in Flink.
   */
  stageID: Int,
  /**
    * Parallelism of this compute stage.
    */
  var numberOfPartitions: Int,
  /**
    * Optional function that can query the state of the resources available for the application.
    */
  resourceStateHandler: Option[() => Int] = None)
extends Logger with Serializable {

  val f: PartitionerFactory = Configuration.internal().getString("repartitioning.partitioner-factory") match {
    case "hu.sztaki.drc.partitioner.KeyIsolatorPartitioner.Factory" => hu.sztaki.drc.partitioner.KeyIsolatorPartitioner.Factory
    case "hu.sztaki.drc.partitioner.GedikPartitioner.ScanFactory" => hu.sztaki.drc.partitioner.GedikPartitioner.ScanFactory
    case "hu.sztaki.drc.partitioner.GedikPartitioner.ReadjFactory" => hu.sztaki.drc.partitioner.GedikPartitioner.ReadjFactory
    case "hu.sztaki.drc.partitioner.GedikPartitioner.RedistFactory" => hu.sztaki.drc.partitioner.GedikPartitioner.RedistFactory
    case _ => throw new RuntimeException("Partitioner factory class is not configured!")
  }

  println(s"#### Partitioner factory type: $f")
  /**
    * Stores the receives histogram.
    */
  protected val histograms = mutable.HashMap[Int, Sampling]()
  /**
    * Current version of the active partitioner.
    */
  protected var currentVersion: Int = 0

  /**
    * @todo Find out the number of partitions.
    * @todo Add a switch for other partitioners, for example Gedik.
    */
  protected var repartitioner: Option[Updateable] = None

  /**
    * Number of repartitions happened so far for this stage.
    */
  protected var repartitionCount: Int = 0

  /**
    * Number of desired partitions to have.
    * @note The decision logic can scale on demand. The underlying engine should provide
    *       fission capabilities. If not, than the system should overwrite this logic by using
    *       `repartitioning.streaming.force-slot-size` configuration.
    */
  protected var nDesiredPartitions: Int = numberOfPartitions
  println(s"### numberOfPartitions: $numberOfPartitions")

  /**
    * Partitioner history.
    */
  protected val broadcastHistory = ArrayBuffer[Partitioner]()

  /**
    * Latest partitioning information, mostly to decide whether a new one is necessary.
    */
  protected var latestPartitioningInfo: Option[PartitioningInfo] = None

  /**
    * Depth of the probability-cut in the new {{KeyIsolatorPartitioner}}.
    */
  protected val treeDepthHint =
    Configuration.get().internal.getInt("repartitioning.partitioner-tree-depth")

  /**
    * Adjusts the global histogram size limit when it is cut from local histograms.
    */
  protected val keyExcessMultiplier: Int =
    Configuration.get().internal.getInt("repartitioning.excess-key-multiplier")
  /**
    * Size limit of the global histogram.
    * More keys used might lead to better partitioner.
    */
  protected def globalHistogramSizeLimit: Int = Math.max((nDesiredPartitions * keyExcessMultiplier) + 1, 2500)

  /**
    * Current, active global histogram that has been computed from the
    * histograms, stored in this variable.
    */
  protected var currentGlobalHistogram: Option[scala.collection.Seq[(Any, Double)]] = None

  protected val testData: Option[Seq[Seq[Int]]] = {
    val testDataPath = """C:\Users\szape\Work\Projects\ER-DR\data\generated.txt"""
    val batchSize = 100000

    def readLines(path: String): Seq[String] = {
      Source.fromFile(path).getLines.toSeq
    }

    def parseLine(line: String): Int = {
      line.substring(1, line.length - 3).toInt
    }

    if(Configuration.internal().getBoolean("repartitioning.streaming.run-simulator"))
      Some(readLines(testDataPath).map(parseLine).grouped(batchSize).toSeq)
    else
      None
  }

  protected val hashBalances: ArrayBuffer[Double] = ArrayBuffer[Double]()
  protected val kIPBalances: ArrayBuffer[Double] = ArrayBuffer[Double]()
  protected val shiftedKIPBalances: ArrayBuffer[Double] = ArrayBuffer[Double]()
  protected val hashPartitioner = new HashPartitioner(numberOfPartitions)

//  println(s"#### hash balance (train): ${hashBalances.mkString("[", ", ", "]")} (version: $currentVersion)")

  /**
    * Fetches the number of total slots available from an external resource-state handler.
    *
    * Should be only called on the driver when decision is to be made.
    */
  protected def totalSlots: Int = {
    resourceStateHandler
      .map(_.apply())
      .getOrElse(throw new Exception(
        "Resource state handler is not available! Maybe called from executor?")
      )
  }

  /**
    * Any decider strategy should accept incoming histograms as {{DataCharacteristicsAccumulator}}s
    * for its partitions.
    */
  def onHistogramArrival(partitionID: Int, keyHistogram: Sampling): Unit

  /**
    * Validates a global histogram whether it fulfills the requirements for a sane
    * repartitioning.
    */
  protected def isValidHistogram(histogram: scala.collection.Seq[(Any, Double)]): Boolean = {
    if (histogram.size < 2) {
      logWarning(s"Histogram size is [${histogram.size}]. Invalidated.")
      false
    } else if (!histogram.forall(!_._2.isInfinity)) {
      logWarning(s"There is an infinite value in the histogram! Invalidated.")
      false
    } else {
      true
    }
  }

  /**
    * @todo Investigate problem regarding empty partition generated by Spark Streaming!
    *       Currently the `numberOfPartitions` is hacked with a -1.
    */
  protected def getPartitioningInfo(
      globalHistogram: scala.collection.Seq[(Any, Double)]): PartitioningInfo = {
    if (Configuration.internal().getBoolean("repartitioning.streaming.force-slot-size")) {
      nDesiredPartitions = totalSlots
    } else if (Configuration.internal().getBoolean("repartitioning.streaming.optimize-parallelism")) {
      val initialInfo =
        PartitioningInfo.newInstance(globalHistogram, totalSlots, treeDepthHint)
      val multiplier = math.max(initialInfo.level / initialInfo.sortedValues.head, 1)
      nDesiredPartitions = (totalSlots * multiplier.ceil).toInt
    }
    logInfo(s"number of slots: $totalSlots, number of partitions: $nDesiredPartitions")
    val partitioningInfo =
      PartitioningInfo.newInstance(globalHistogram.take(keyExcessMultiplier * nDesiredPartitions), nDesiredPartitions, treeDepthHint)
    logInfo(s"Constructed partitioning info is [$partitioningInfo].")
    logObject(("partitioningInfo", stageID, partitioningInfo))
    latestPartitioningInfo = Some(partitioningInfo)
    partitioningInfo
  }


  protected def clearHistograms(): Unit

  /**
    * Decides if repartitioning is needed. If so, constructs a new
    * partitioning function and sends the strategy to each worker.
    * It asks the RepartitioningTrackerMaster to broadcast the new
    * strategy to workers.
    */
  def repartition(): Boolean = {
    beforeRepartition()
    val doneRepartitioning =
      if (preDecide()) {
        val globalHistogram = getGlobalHistogram
        if (decideAndValidate(globalHistogram)) {
          resetPartitioners(getNewPartitioner(getPartitioningInfo(globalHistogram)))
          true
        } else {
          logInfo("Decide-and-validate is no-go.")
          false
        }
      } else {
        logInfo("Pre-decide is no-go.")
        false
      }
    cleanup()
    doneRepartitioning
  }

  protected def getGlobalHistogram = {
    currentGlobalHistogram.getOrElse(computeGlobalHistogram)
  }

  protected def beforeRepartition(): Unit = {

  }

  protected def preDecide(): Boolean

  /**
    * @todo Use `h.update` instead of `h.value` in batch job.
    */
  protected def computeGlobalHistogram: scala.collection.Seq[(Any, Double)] = {
    val numRecords = histograms.values.map(_.recordsPassed).sum
    logInfo(s"### records contributed to this histogram: $numRecords")
    println(s"### histograms size: ${histograms.size}")
    histograms.values.foreach(v => {
      //      println(s"### h.recordsPassed: ${v.recordsPassed}")
      //      println(s"### h.normalizationFactor: ${v.normalizationFactor}")
      //      println(s"### h.samplingRate: ${v.sampleRate}")
      //      println(s"### h_size: ${v.value.values.sum}")
      println(s"### h_normalised: ${v.normalize(v.recordsPassed).values.sum}")
    })

//    val normalizationFactors = histograms.values.map(_.normalizationFactor).sum
//    println(s"### numRecords: $numRecords")
//    println(s"### numRecords: $normalizationFactors")

    val globalHistogram =
      histograms.values.map(h => h.normalize(numRecords))
        .reduce(Naive.merge[Any, Double](0.0)(
          (a: Double, b: Double) => a + b)
        )
        .toSeq.sortBy(-_._2).take(globalHistogramSizeLimit)
//    println(s"### global histogram size: ${globalHistogram.map(_._2).sum}")
    logObject(("globalHistogram", stageID, globalHistogram.take(10)))
    logInfo(
      globalHistogram.take(10).foldLeft(
        s"Global histogram for repartitioning " +
        s"step $repartitionCount:\n")((x, y) =>
        x + s"\t${y._1}\t->\t${y._2}\n"))
    currentGlobalHistogram = Some(globalHistogram)
    globalHistogram
  }

  protected def decideAndValidate(globalHistogram: scala.collection.Seq[(Any, Double)]): Boolean

  protected def getNewPartitioner(partitioningInfo: PartitioningInfo): Partitioner = {
    def writeOut(record: Any): Unit = {
      val writer = new PrintWriter(
        new BufferedWriter(
          new FileWriter(new File("""C:\Users\szape\Work\Projects\ER-DR\data\HistogramHeads.txt"""), true)))
      writer.println(record)
      //			writer.println("----")
      writer.close()
    }

    val sortedKeys = partitioningInfo.sortedKeys

    repartitioner = repartitioner match {
      case Some(rp) => Some(rp.update(partitioningInfo))
      case None => Some(f.apply(nDesiredPartitions).update(partitioningInfo))
    }

    // ********************************************** debug code
    val maxPart = repartitioner.get.get((1, 1))
    var goesToMaxPart = Seq[Int]()
    for (i <- 1 to 1000) {
      if (repartitioner.get.get((i, 1)) == maxPart) {
        goesToMaxPart = goesToMaxPart :+ i
      }
    }
    println(s"###Partition of '1': $maxPart, keys that went to the same partition as '1': $goesToMaxPart")
    val secondPart = repartitioner.get.get((2, 1))
    var goesToSecondPart = Seq[Int]()
    for (i <- 1 to 1000) {
      if (repartitioner.get.get((i, 1)) == secondPart) {
        goesToSecondPart = goesToSecondPart :+ i
      }
    }
    println(s"###Partition of '2': $secondPart, keys that went to the same partition as '1': $goesToSecondPart")

    logInfo(s"Decided to repartition stage $stageID.")
    currentVersion += 1

    writeOut(s"${sortedKeys.head} ($currentVersion)")

    def calculateBalance(data: Seq[Int], dataSize: Int, part: Partitioner): Double = {
      numberOfPartitions * data.map(r => (r, 1)).groupBy(part.get).values.map(_.size).max.toDouble / dataSize
    }

    //    sortedKeys.foreach {
    //      key => logInfo(s"Key $key went to ${repartitioner.get.get(key)}.")
    //    }
    if(Configuration.internal().getBoolean("repartitioning.streaming.run-simulator")) {
      logInfo("Partitioner created, simulating run with global histogram.")


      implicit val ec = ExecutionContext.global
      val result: Future[(Double, Double, Double)] = Future {
        val batchSize: Int = 100000
        def data(i: Int): Seq[Int] = (testData.get)(i)
//        val data1: Seq[Int] = (testData.get) (currentVersion - 1) // batches(currentVersion)
//        val data2: Seq[Int] = (testData.get) (currentVersion - 1)
//        val partitionsWithLoad: Seq[(Int, Double)] = data.map(repartitioner.get.get)
//          .groupBy(identity)
//          .mapValues(_.size.toDouble / batchSize)
//          .toSeq
//          .sortBy(_._1)
//        val loads: Seq[Double] = partitionsWithLoad.map(_._2).sorted // apply partitioner to data, count values in the list and sort (.groupBy(identity).mapValues(_.size))
//        val balance: Double = numberOfPartitions * loads.max // compute balance, compare with HashPartitioner
        (calculateBalance(data(currentVersion - 1), batchSize, hashPartitioner),
          calculateBalance(data(currentVersion - 1), batchSize, repartitioner.get),
          calculateBalance(data(currentVersion), batchSize, repartitioner.get))
      }

      result onComplete {
        case Success(res) =>
          logInfo(s"Simulation completed for version $currentVersion")
          hashBalances += res._1
          kIPBalances += res._3
          shiftedKIPBalances += res._2
//          println(s"#### current version before if is $currentVersion")
          if(currentVersion == 49) {
//            calculateBalance((testData.get)(0), 100000, hashPartitioner)
//            val dataPerPartitions = (testData.get)(0).map(hashPartitioner.get).groupBy(identity).map(s => (s._1, s._2.size)).toSeq.sortBy(_._1)
//            val dataPerPartitions2 = numberOfPartitions * (testData.get)(0).map(hashPartitioner.get).groupBy(identity).values.map(_.size).max / 100000
//            println(s"#### current version inside if is $currentVersion")
            println(s"### hash balance (train): ${hashBalances.mkString("[", ", ", "]")} (version: $currentVersion)")
            println(s"### KIP balance (test): ${kIPBalances.mkString("[", ", ", "]")} (version: $currentVersion)")
            println(s"### KIP balance (train): ${shiftedKIPBalances.mkString("[", ", ", "]")} (version: $currentVersion)")
          }
//          println(s"### loads: ${res._1} (version: $currentVersion)")
//          println(s"### balance: ${res._2} (version: $currentVersion)")
        case Failure(t) => println("An error has occurred: " + t.getMessage)
      }

//      val batchSize: Int = 100000
//      val data: Seq[Int] = (testData.get)(currentVersion) // batches(currentVersion)
//      val partitionsWithLoad: Seq[(Int, Double)] = data.map(repartitioner.get.get)
//        .groupBy(identity)
//        .mapValues(_.size.toDouble / batchSize)
//        .toSeq
//        .sortBy(_._1)
//      val loads: Seq[Double] = partitionsWithLoad.map(_._2).sorted // apply partitioner to data, count values in the list and sort (.groupBy(identity).mapValues(_.size))
//      val balance: Double = numberOfPartitions * loads.max// compute balance, compare with HashPartitioner
//      println(s"### loads: $loads (version: $currentVersion)")
//      println(s"### balance: $balance (version: $currentVersion)")
    }

    repartitioner.getOrElse(throw new RuntimeException("Partitioner is `None` after repartitioning. This was unexpected!"))
  }

  protected def resetPartitioners(newPartitioner: Partitioner): Unit

  protected def cleanup(): Unit = {
    currentGlobalHistogram = None
  }
}

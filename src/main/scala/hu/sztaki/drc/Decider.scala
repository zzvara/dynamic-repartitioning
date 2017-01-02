package hu.sztaki.drc

import hu.sztaki.drc.partitioner.{KeyIsolationPartitioner, Partitioner, PartitioningInfo, WeightedHashPartitioner}
import hu.sztaki.drc.utilities.{Configuration, Exception, Logger}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
  * A decider strategy, that continuously receives histograms from the physical tasks
  * and decides when and how to repartition a certain stage of computation.
  *
  * That means a decider strategy is always bound to a specific stage in batch mode, or
  * to a stream in streaming mode. A stageID of Spark should be a vertexID in Flink.
  *
  * @todo Add support to check distance from uniform distribution.
  * @param stageID A Spark stage ID, stream ID or a vertex ID in Flink.
  * @param resourceStateHandler Optional function that can query the state of the resources
  *                             available for the application.
  */
abstract class Decider(
  stageID: Int,
  resourceStateHandler: Option[() => Int] = None)
extends Logger with Serializable {
  protected val histograms = mutable.HashMap[Int, Sampler]()
  protected var currentVersion: Int = 0

  /**
    * Number of repartitions happened.
    */
  protected var repartitionCount: Int = 0

  /**
    * Partitioner history.
    */
  protected val broadcastHistory = ArrayBuffer[Partitioner]()

  /**
    * Number of desired partitions to have.
    */
  protected var numberOfPartitions: Int = 1

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
    * Current, active global histogram that has been computed from the
    * histograms, stored in this variable.
    */
  protected var currentGlobalHistogram: Option[scala.collection.Seq[(Any, Double)]] = None

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
  def onHistogramArrival(partitionID: Int, keyHistogram: Sampler): Unit

  /**
    * Validates a global histogram whether it fulfills the requirements for a sane
    * repartitioning.
    */
  protected def isValidHistogram(histogram: scala.collection.Seq[(Any, Double)]): Boolean = {
    if (histogram.size < 2) {
      logWarning(s"Histogram size is ${histogram.size}. Invalidated.")
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
    val initialInfo =
      PartitioningInfo.newInstance(globalHistogram, totalSlots, treeDepthHint)
    val multiplier = math.min(initialInfo.level / initialInfo.sortedValues.head, 2)
    numberOfPartitions = (totalSlots * multiplier.ceil.toInt) - 1
    if (Configuration.internal().getBoolean("repartitioning.streaming.force-slot-size")) {
      numberOfPartitions = totalSlots - 1
    }
    val partitioningInfo =
      PartitioningInfo.newInstance(globalHistogram, numberOfPartitions, treeDepthHint)
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
    val numRecords =
      histograms.values.map(_.recordsPassed).sum

    val globalHistogram =
      histograms.values.map(h => h.normalize(h.value, numRecords))
        .reduce(Sampler.merge[Any, Double](0.0)(
          (a: Double, b: Double) => a + b)
        )
        .toSeq.sortBy(-_._2).take(50)
    logObject(("globalHistogram", stageID, globalHistogram))
    logInfo(
      globalHistogram.foldLeft(
        s"Global histogram for repartitioning " +
        s"step $repartitionCount:\n")((x, y) =>
        x + s"\t${y._1}\t->\t${y._2}\n"))
    currentGlobalHistogram = Some(globalHistogram)
    globalHistogram
  }

  protected def decideAndValidate(globalHistogram: scala.collection.Seq[(Any, Double)]): Boolean

  protected def getNewPartitioner(partitioningInfo: PartitioningInfo): Partitioner = {
    val sortedKeys = partitioningInfo.sortedKeys

    val repartitioner = new KeyIsolationPartitioner(
      partitioningInfo,
      WeightedHashPartitioner.newInstance(partitioningInfo, (key: Any) =>
        (MurmurHash3.stringHash((key.hashCode + 123456791).toString).toDouble
          / Int.MaxValue + 1) / 2)
    )

    logInfo("Partitioner created, simulating run with global histogram.")
    sortedKeys.foreach {
      key => logInfo(s"Key $key went to ${repartitioner.get(key)}.")
    }

    logInfo(s"Decided to repartition stage $stageID.")
    currentVersion += 1

    repartitioner
  }

  protected def resetPartitioners(newPartitioner: Partitioner): Unit

  protected def cleanup(): Unit = {
    currentGlobalHistogram = None
  }
}

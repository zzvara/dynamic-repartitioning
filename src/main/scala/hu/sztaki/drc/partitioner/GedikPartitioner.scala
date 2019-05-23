package hu.sztaki.drc.partitioner

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.google.common.collect.TreeMultiset
import com.google.common.hash.Hashing
import hu.sztaki.drc.utilities.Distribution

import scala.collection.JavaConversions._
import scala.collection.immutable.{HashMap, TreeMap}
import scala.collection.mutable

/**
  * Partitioning function construction based on paper by Gedik et al. (2014):
  * Partitioning Functions for Stateful Data Parallelism in Stream Processing.
  */
object GedikPartitioner {

  implicit object ScanFactory extends PartitionerFactory {
    override def apply(numPartitions: Int): GedikPartitioner = {
      val betaS: Double => Double = x => x
      val betaC: Double => Double = x => x
      val thetaS = 0.2
      val thetaC = 0.2
      val thetaN = 0.2
      val utility: (Double, Double) => Double = {
        case (balancePenalty, migrationPenalty) => balancePenalty + migrationPenalty
      }

      val hash = Hashing.murmur3_32()
      val hashFunc: Any => Int = {
        case (x: String) =>
          hash.hashString(x, Charset.defaultCharset()).asInt()
        case (x: Int) =>
          hash.hashInt(x).asInt()
        case (i: Int, j: Int) =>
          val bytes = ByteBuffer.allocate(8)
          bytes.putInt(0, i)
          bytes.putInt(4, j)
          hash.hashBytes(bytes.array()).asInt()
        case _ =>
          throw new UnsupportedOperationException("Currently we can only hash Int or String.")
      }

      new GedikPartitioner(numPartitions, 100, hashFunc,
        betaS, betaC, thetaS, thetaC, thetaN, utility, "Scan")
    }
  }

  implicit object ReadjFactory extends PartitionerFactory {
    override def apply(numPartitions: Int): GedikPartitioner = {
      val betaS: Double => Double = x => x
      val betaC: Double => Double = x => x
      val thetaS = 0.2
      val thetaC = 0.2
      val thetaN = 0.2
      val utility: (Double, Double) => Double = {
        case (balancePenalty, migrationPenalty) => balancePenalty + migrationPenalty
      }

      val hash = Hashing.murmur3_32()
      val hashFunc: Any => Int = {
        case (x: String) =>
          hash.hashString(x, Charset.defaultCharset()).asInt()
        case (x: Int) =>
          hash.hashInt(x).asInt()
        case (i: Int, j: Int) =>
          val bytes = ByteBuffer.allocate(8)
          bytes.putInt(0, i)
          bytes.putInt(4, j)
          hash.hashBytes(bytes.array()).asInt()
        case _ =>
          throw new UnsupportedOperationException("Currently we can only hash Int or String.")
      }

      new GedikPartitioner(numPartitions, 100, hashFunc,
        betaS, betaC, thetaS, thetaC, thetaN, utility, "Readj")
    }
  }

  implicit object RedistFactory extends PartitionerFactory {
    override def apply(numPartitions: Int): GedikPartitioner = {
      val betaS: Double => Double = x => x
      val betaC: Double => Double = x => x
      val thetaS = 0.2
      val thetaC = 0.2
      val thetaN = 0.2
      val utility: (Double, Double) => Double = {
        case (balancePenalty, migrationPenalty) => balancePenalty + migrationPenalty
      }

      val hash = Hashing.murmur3_32()
      val hashFunc: Any => Int = {
        case (x: String) =>
          hash.hashString(x, Charset.defaultCharset()).asInt()
        case (x: Int) =>
          hash.hashInt(x).asInt()
        case (i: Int, j: Int) =>
          val bytes = ByteBuffer.allocate(8)
          bytes.putInt(0, i)
          bytes.putInt(4, j)
          hash.hashBytes(bytes.array()).asInt()
        case _ =>
          throw new UnsupportedOperationException("Currently we can only hash Int or String.")
      }

      new GedikPartitioner(numPartitions, 100, hashFunc,
        betaS, betaC, thetaS, thetaC, thetaN, utility, "Redist")
    }
  }

  def main(args: Array[String]): Unit = {
    val numReplicas = 10
    val numPartitions: Int = 20
    val keys = ('a' to 'n').map(_.toString)

    val weighting =
    // Distribution.zeta(1, 1.5, numPartitions).probabilities
      keys.zip(Distribution.twoStep(numPartitions.toDouble / 3.0d, numPartitions).probabilities)

    val newWeighting: Array[Double] =
    // Distribution.zeta(2, 5, numPartitions).probabilities
      Distribution.twoStep(numPartitions.toDouble / 2.0d, numPartitions).probabilities

    val consistentHasher =
      new ConsistentHasher[Int](numPartitions, _.hashCode(), numReplicas)

    val initHasher: String => Int = consistentHasher.getPartition

    val partitioner = constructPartitionerScan(numPartitions, consistentHasher,
      weighting, weighting,
      initHasher, x => 1, x => 1, 0.1, 0.1, 1.0, _ + _)

    val y = keys.groupBy(x => partitioner(x)).mapValues(ks => ks.map(weighting.toMap).sum)

    println(y)

  }

  class ConsistentHasher[HashCode](partitions: Int,
    hashFunc: Any => HashCode,
    numReplicas: Int)
    (implicit ord: Ordering[HashCode])
    extends Partitioner {

    override def numPartitions: Int = partitions

    private val hashes =
      for {
        partition <- 0 until partitions
        replica <- 0 until numReplicas
      } yield {
        // TODO better way to combine partition with replica?
        hashFunc((partition, replica)) -> partition
      }

    private val lookupMap: TreeMap[HashCode, Int] = TreeMap[HashCode, Int](hashes: _*)(ord)

    private val jLookupMap: java.util.TreeMap[HashCode, Int] =
      new java.util.TreeMap(mapAsJavaMap(lookupMap))

    override def getPartition(key: Any): Int = {
      // TODO this lookup should be more efficient, maybe use Java TreeMap or TreeSet?
      // val partitionOld = lookupMap.valuesIteratorFrom(hashFunc(key)).toIterable.headOption
      val entry = jLookupMap.ceilingEntry(hashFunc(key))
      if (entry == null) {
        jLookupMap.firstEntry().getValue
      } else {
        entry.getValue
      }
    }

    override def toString: String = s"ConsistentHasher($id)"

  }

  def indicator(b: Boolean): Int = if (b) 1 else 0

  class GedikPartitioner(partitions: Int,
    consistentHash: Partitioner,
    betaS: Double => Double,
    betaC: Double => Double,
    thetaS: Double,
    thetaC: Double,
    thetaN: Double,
    utility: (Double, Double) => Double,
    currFreqsSorted: Seq[(Any, Double)] = Seq(),
    prevFreqsSorted: Seq[(Any, Double)] = Seq(),
    prevP: Option[Partitioner] = None,
    algorithm: String)
    extends Updateable {

    def this(partitions: Int,
      numReplicasConsistentHash: Int,
      hashFunc: Any => Int,
      betaS: Double => Double,
      betaC: Double => Double,
      thetaS: Double,
      thetaC: Double,
      thetaN: Double,
      utility: (Double, Double) => Double,
      algorithm: String) {
      this(partitions,
        new ConsistentHasher[Int](partitions, hashFunc, numReplicasConsistentHash),
        betaS, betaC, thetaS, thetaC, thetaN, utility, Seq(), Seq(), None, algorithm)
    }

    override def numPartitions: Int = partitions

    private val prevPartitioner: Any => Int =
      prevP match {
        case Some(p) => p.getPartition
        case None => consistentHash.getPartition
      }

    private val partitionerConstructor: (Int, Partitioner, Seq[(Any, Double)],
      Seq[(Any, Double)], (Any) => Int, (Double) => Double, (Double) => Double,
      Double, Double, Double, (Double, Double) => Double) => (Any) => Int =
      algorithm match {
        case "Scan" =>
          constructPartitionerScan[Any]
        case "Redist" =>
          constructPartitionerRedist[Any]
        case "Readj" =>
          GedikPartitioner2.constructPartitionerReadj[Any]
        case _ =>
          throw new IllegalArgumentException(s"Gedik algorithm should be either Scan or Readj")
      }

    private val partitioner = partitionerConstructor(partitions, consistentHash,
      currFreqsSorted, prevFreqsSorted, prevPartitioner, betaS, betaC,
      thetaS, thetaC, thetaN, utility)

    override def getPartition(key: Any): Int = partitioner(key)

    override def update(partitioningInfo: PartitioningInfo): GedikPartitioner = {
      new GedikPartitioner(partitioningInfo.partitions, consistentHash,
        betaS, betaC, thetaS, thetaC, thetaN, utility,
        currFreqsSorted = partitioningInfo.heavyKeys,
        prevFreqsSorted = currFreqsSorted,
        prevP = Some(this), algorithm)
    }

    override def toString: String = s"GedikPartitioner($algorithm, $id)"

  }

  /**
    *
    * Note:
    * Partitions are indexed from 0 to (numPartitions - 1).
    * This is in contrast to the paper where the first index is 1.
    *
    * @param numPartitions
    * Number of partitions.
    * @param currFreqsSorted
    * Measured frequencies of top elements. Must be sorted in descending order.
    * @param prevFreqsSorted
    * Measured frequencies of top elements in previous partitioner. Must be sorted in descending order.
    * @param prevP
    * Previous partitioner function.
    * @param betaS
    * Memory (state) cost by frequency. Denoted as \beta_s in paper.
    * @param betaC
    * Computation cost by frequency. Denoted as \beta_c in paper.
    * @param thetaS
    * Balance constraint for memory (state) load.
    * @param thetaC
    * Balance constraint for computation load.
    * @param thetaN
    * Balance constraint for communication (network) load.
    * @param utility
    * Utility function (U) to set the importance of balance penalty (a) vs migration penalty (\gamma).
    * @tparam K
    * Type of key to be partitioned.
    * @return
    * Constructed partitioner.
    */
  def constructPartitionerRedist[K](numPartitions: Int,
    consistentHasher: Partitioner,
    currFreqsSorted: Seq[(K, Double)],
    prevFreqsSorted: Seq[(K, Double)],
    prevP: K => Int,
    betaS: Double => Double,
    betaC: Double => Double,
    thetaS: Double,
    thetaC: Double,
    thetaN: Double,
    utility: (Double, Double) => Double): K => Int = {

    val prevFreqsMap = prevFreqsSorted.toMap
    val currFreqsMap = currFreqsSorted.toMap

    // Balance penalty index for faster updates
    case class BalancePenaltyMutable(theta: Double,
      loadFunc: Double => Double,
      numPartitions: Int) {

      private val loadsSorted =
        TreeMultiset.create[(Int, Double)](Ordering.by[(Int, Double), Double](_._2))

      // init with 0 load at every partition
      for (i <- 0 until numPartitions) {
        loadsSorted.add((i, 0.0))
      }

      private val loads: Array[Double] = Array.fill(numPartitions)(0.0)

      private var loadSum: Double = 0.0

      def penalty: Double = {
        val avgLoad = loadSum / loads.length
        val maxLoad = loadsSorted.lastEntry().getElement._2
        val minLoad = loadsSorted.firstEntry().getElement._2
        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def penaltyWithAddedWeight(partition: Int, weight: Double): Double =
        penaltyWithAddedLoad(partition, loadFunc(weight))

      private def penaltyWithAddedLoad(partition: Int, load: Double): Double = {

        val avgLoad = (loadSum + load) / loads.length
        val prevLoad = loads(partition)
        val currLoad = prevLoad + load

        val firstEntry = loadsSorted.firstEntry()
        val (prevMaxPartition, prevMaxLoad) = loadsSorted.lastEntry().getElement
        val (prevMinPartition, prevMinLoad) = firstEntry.getElement

        val maxLoad =
          if (prevMaxPartition == partition) {
            currLoad
          } else {
            Math.max(currLoad, prevMaxLoad)
          }

        val minLoad =
          if (prevMinPartition == partition && firstEntry.getCount == 1) {
            if (loadsSorted.size() > 1) {
              val inOrderIter = loadsSorted.iterator()
              inOrderIter.next()

              val possibleMinLoad = inOrderIter.next()._2
              // TODO min???
              Math.min(currLoad, possibleMinLoad)
            } else {
              currLoad
            }
          } else {
            prevMinLoad
          }

        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def addWeightToPartition(partition: Int, weight: Double): Unit =
        addLoadToPartition(partition, loadFunc(weight))

      private def addLoadToPartition(partition: Int, load: Double): Unit = {
        val prevLoad = loads(partition)
        val currLoad = prevLoad + load

        loads(partition) = currLoad

        loadsSorted.remove(partition -> prevLoad)
        loadsSorted.add(partition -> currLoad)

        loadSum += currLoad
      }
    }

    // H_c in paper
    val consistentHash: K => Int = consistentHasher.getPartition

    // TODO note: we use here the latest known frequencies, i.e. freqs in currFreqs if available,
    // TODO For items in both currFreqs and prevFreqs the prevFreqs is avoided.
    // TODO Check paper whether this is the intended behaviour.

    // D_o^(t+1) in paper
    val oldFreqs = prevFreqsMap -- currFreqsMap.keys
    // D_a^(t+1) in paper
    val allFreqs = oldFreqs ++ currFreqsMap

    // m in paper, migration cost due to items not being tracked anymore
    val untrackedMigrationCost = oldFreqs
      .map { case (key, freq) => betaS(freq) * indicator(prevP(key) != consistentHash(key)) }
      .sum

    // m^_ in paper
    val idealMigrationCost = allFreqs.values.map(betaS(_)).sum / numPartitions


    def loadFuncS(freq: Double) = betaS(freq)

    def loadFuncC(freq: Double) = freq * betaC(freq)

    // the communication cost is considered linear, see paper for more details
    def loadFuncN(freq: Double) = freq

    object BalancePenalty {

      def create(theta: Double, loadFunc: Double => Double, numPartitions: Int): BalancePenalty =
        BalancePenalty(theta, loadFunc, numPartitions,
          loadsSorted = SortedTree.emptyPartitions(numPartitions),
          loads = Seq.fill(numPartitions)(0.0),
          loadSum = 0.0
        )
    }
    case class BalancePenalty private(theta: Double,
      loadFunc: Double => Double,
      numPartitions: Int,
      private val loadsSorted: SortedTree,
      private val loads: Seq[Double],
      private val loadSum: Double) {

      def penalty: Double = {
        val avgLoad = loadSum / loads.length
        val maxLoad = loadsSorted.min._2
        val minLoad = loadsSorted.max._2
        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def withAddedWeights(ws: Seq[(Int, Double)]): BalancePenalty =
        withAddedLoads(ws.map {
          case (partition, weight) => (partition, Math.signum(weight) * loadFunc(Math.abs(weight)))
        })

      private def withAddedLoads(addedLoads: Seq[(Int, Double)]): BalancePenalty = {
        addedLoads.foldLeft(this) {
          case (prev, (partition, plusLoad)) =>
            val prevLoad = prev.loads(partition)
            val currLoad = prevLoad + plusLoad
            prev.copy(
              loadsSorted = prev.loadsSorted - (partition -> prevLoad) + (partition -> currLoad),
              loads = prev.loads.updated(partition, currLoad),
              loadSum = prev.loadSum + plusLoad
            )
        }
      }
    }

    var balancePenaltyS = BalancePenalty.create(thetaS, loadFuncS, numPartitions)
    var balancePenaltyC = BalancePenalty.create(thetaC, loadFuncC, numPartitions)
    var balancePenaltyN = BalancePenalty.create(thetaN, loadFuncN, numPartitions)

    var balancePenalties = Seq(balancePenaltyS, balancePenaltyC, balancePenaltyN)

    def balancePenaltyByExplicitHash(h: Map[K, Int]): Double = {

      // inverting the map
      // TODO optimization: could probably construct an array in a more efficient way
      val inverseMap = (0 until numPartitions).map(p => (p, Iterable())).toMap ++ h.groupBy(_._2)
        .mapValues(_.keys)
      val partitions: Seq[Iterable[K]] = Seq.tabulate(numPartitions)(inverseMap)

      def resourceBalancePenalty(loadFunc: Double => Double, theta: Double): Double = {
        val keyToLoad: K => Double = x => loadFunc(currFreqsMap(x))
        val loads = partitions.map(_.map(keyToLoad).sum)

        val avgLoad = loads.sum / loads.size

        (loads.max - loads.min) / (theta * avgLoad)
      }

      Math.cbrt(
        resourceBalancePenalty(loadFuncS, thetaS) *
          resourceBalancePenalty(loadFuncC, thetaC) *
          resourceBalancePenalty(loadFuncN, thetaN)
      )
    }

    val toBePlaced = mutable.HashSet(currFreqsSorted: _*)

    var m = untrackedMigrationCost
    val explicitHash = mutable.HashMap[K, Int]()

    while (toBePlaced.nonEmpty) {
      // best placement, initially invalid
      var j = -1
      // best item to place
      var d: Option[K] = None
      var dFreq: Option[Double] = None
      // best utility value, lower is better
      var u = Double.MaxValue

      // for each candidate
      for ((c, freq) <- toBePlaced) {
        // old location
        val h = prevP(c)

        // for each placement
        for (l <- 0 until numPartitions) {

          val balancePenalty = Math.cbrt(
            balancePenalties.map(_.withAddedWeights(Seq(l -> freq)).penalty).product
          )

          //            val balancePenalty = balancePenaltyByExplicitHash(explicitHash + (key -> l))

          val migrationPenalty = (m + betaS(freq) * indicator(l != h)) / idealMigrationCost
          val utilityOfPlacement = utility(balancePenalty, migrationPenalty) / freq

          if (utilityOfPlacement < u) {
            j = l
            d = Some(c)
            dFreq = Some(freq)
            u = utilityOfPlacement
          }
        }
      }

      balancePenalties = balancePenalties.map(_.withAddedWeights(Seq(j -> dFreq.get)))
      explicitHash += d.get -> j
      val h = prevP(d.get)
      m += betaS(dFreq.get) * indicator(j != h)
      toBePlaced.remove((d.get, dFreq.get))
    }

    combineExplicitAndConsistent(explicitHash.toMap, consistentHash)
  }

  /**
    *
    * Note:
    * Partitions are indexed from 0 to (numPartitions - 1).
    * This is in contrast to the paper where the first index is 1.
    *
    * @param numPartitions
    * Number of partitions.
    * @param currFreqsSorted
    * Measured frequencies of top elements. Must be sorted in descending order.
    * @param prevFreqsSorted
    * Measured frequencies of top elements in previous partitioner. Must be sorted in descending order.
    * @param prevP
    * Previous partitioner function.
    * @param betaS
    * Memory (state) cost by frequency. Denoted as \beta_s in paper.
    * @param betaC
    * Computation cost by frequency. Denoted as \beta_c in paper.
    * @param thetaS
    * Balance constraint for memory (state) load.
    * @param thetaC
    * Balance constraint for computation load.
    * @param thetaN
    * Balance constraint for communication (network) load.
    * @param utility
    * Utility function (U) to set the importance of balance penalty (a) vs migration penalty (\gamma).
    * @tparam K
    * Type of key to be partitioned.
    * @return
    * Constructed partitioner.
    */
  def constructPartitionerScan[K](numPartitions: Int,
    consistentHasher: Partitioner,
    // fixme use Seq here
    currFreqsSorted: Seq[(K, Double)],
    prevFreqsSorted: Seq[(K, Double)],
    prevP: K => Int,
    betaS: Double => Double,
    betaC: Double => Double,
    thetaS: Double,
    thetaC: Double,
    thetaN: Double,
    utility: (Double, Double) => Double): K => Int = {

    val prevFreqsMap = prevFreqsSorted.toMap
    val currFreqsMap = currFreqsSorted.toMap

    // Balance penalty index for faster updates
    case class BalancePenaltyMutable(theta: Double,
      loadFunc: Double => Double,
      numPartitions: Int) {

      private val loadsSorted =
        TreeMultiset.create[(Int, Double)](Ordering.by[(Int, Double), Double](_._2))

      // init with 0 load at every partition
      for (i <- 0 until numPartitions) {
        loadsSorted.add((i, 0.0))
      }

      private val loads: Array[Double] = Array.fill(numPartitions)(0.0)

      private var loadSum: Double = 0.0

      def penalty: Double = {
        val avgLoad = loadSum / loads.length
        val maxLoad = loadsSorted.lastEntry().getElement._2
        val minLoad = loadsSorted.firstEntry().getElement._2
        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def penaltyWithAddedWeight(partition: Int, weight: Double): Double =
        penaltyWithAddedLoad(partition, loadFunc(weight))

      private def penaltyWithAddedLoad(partition: Int, load: Double): Double = {

        val avgLoad = (loadSum + load) / loads.length
        val prevLoad = loads(partition)
        val currLoad = prevLoad + load

        val firstEntry = loadsSorted.firstEntry()
        val (prevMaxPartition, prevMaxLoad) = loadsSorted.lastEntry().getElement
        val (prevMinPartition, prevMinLoad) = firstEntry.getElement

        val maxLoad =
          if (prevMaxPartition == partition) {
            currLoad
          } else {
            Math.max(currLoad, prevMaxLoad)
          }

        val minLoad =
          if (prevMinPartition == partition && firstEntry.getCount == 1) {
            if (loadsSorted.size() > 1) {
              val inOrderIter = loadsSorted.iterator()
              inOrderIter.next()

              val possibleMinLoad = inOrderIter.next()._2
              // TODO min???
              Math.min(currLoad, possibleMinLoad)
            } else {
              currLoad
            }
          } else {
            prevMinLoad
          }

        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def addWeightToPartition(partition: Int, weight: Double): Unit =
        addLoadToPartition(partition, loadFunc(weight))

      private def addLoadToPartition(partition: Int, load: Double): Unit = {
        val prevLoad = loads(partition)
        val currLoad = prevLoad + load

        loads(partition) = currLoad

        loadsSorted.remove(partition -> prevLoad)
        loadsSorted.add(partition -> currLoad)

        loadSum += currLoad
      }
    }

    // H_c in paper
    val consistentHash: K => Int = consistentHasher.getPartition

    // TODO note: we use here the latest known frequencies, i.e. freqs in currFreqs if available,
    // TODO For items in both currFreqs and prevFreqs the prevFreqs is avoided.
    // TODO Check paper whether this is the intended behaviour.

    // D_o^(t+1) in paper
    val oldFreqs = prevFreqsMap -- currFreqsMap.keys
    // D_a^(t+1) in paper
    val allFreqs = oldFreqs ++ currFreqsMap

    // m in paper, migration cost due to items not being tracked anymore
    val untrackedMigrationCost = oldFreqs
      .map { case (key, freq) => betaS(freq) * indicator(prevP(key) != consistentHash(key)) }
      .sum

    // m^_ in paper
    val idealMigrationCost = allFreqs.values.map(betaS(_)).sum / numPartitions


    def loadFuncS(freq: Double) = betaS(freq)

    def loadFuncC(freq: Double) = freq * betaC(freq)

    // the communication cost is considered linear, see paper for more details
    def loadFuncN(freq: Double) = freq

    object BalancePenalty {

      def create(theta: Double, loadFunc: Double => Double, numPartitions: Int): BalancePenalty =
        BalancePenalty(theta, loadFunc, numPartitions,
          loadsSorted = SortedTree.emptyPartitions(numPartitions),
          loads = Seq.fill(numPartitions)(0.0),
          loadSum = 0.0
        )
    }
    case class BalancePenalty private(theta: Double,
      loadFunc: Double => Double,
      numPartitions: Int,
      private val loadsSorted: SortedTree,
      private val loads: Seq[Double],
      private val loadSum: Double) {

      def penalty: Double = {
        val avgLoad = loadSum / loads.length
        val maxLoad = loadsSorted.min._2
        val minLoad = loadsSorted.max._2
        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def withAddedWeights(ws: Seq[(Int, Double)]): BalancePenalty =
        withAddedLoads(ws.map {
          case (partition, weight) => (partition, Math.signum(weight) * loadFunc(Math.abs(weight)))
        })

      private def withAddedLoads(addedLoads: Seq[(Int, Double)]): BalancePenalty = {
        addedLoads.foldLeft(this) {
          case (prev, (partition, plusLoad)) =>
            val prevLoad = prev.loads(partition)
            val currLoad = prevLoad + plusLoad
            prev.copy(
              loadsSorted = prev.loadsSorted - (partition -> prevLoad) + (partition -> currLoad),
              loads = prev.loads.updated(partition, currLoad),
              loadSum = prev.loadSum + plusLoad
            )
        }
      }
    }

    var balancePenaltyS = BalancePenalty.create(thetaS, loadFuncS, numPartitions)
    var balancePenaltyC = BalancePenalty.create(thetaC, loadFuncC, numPartitions)
    var balancePenaltyN = BalancePenalty.create(thetaN, loadFuncN, numPartitions)

    var balancePenalties = Seq(balancePenaltyS, balancePenaltyC, balancePenaltyN)

    def balancePenaltyByExplicitHash(h: Map[K, Int]): Double = {

      // inverting the map
      // TODO optimization: could probably construct an array in a more efficient way
      val inverseMap = (0 until numPartitions).map(p => (p, Iterable())).toMap ++ h.groupBy(_._2)
        .mapValues(_.keys)
      val partitions: Seq[Iterable[K]] = Seq.tabulate(numPartitions)(inverseMap)

      def resourceBalancePenalty(loadFunc: Double => Double, theta: Double): Double = {
        val keyToLoad: K => Double = x => loadFunc(currFreqsMap(x))
        val loads = partitions.map(_.map(keyToLoad).sum)

        val avgLoad = loads.sum / loads.size

        (loads.max - loads.min) / (theta * avgLoad)
      }

      Math.cbrt(
        resourceBalancePenalty(loadFuncS, thetaS) *
          resourceBalancePenalty(loadFuncC, thetaC) *
          resourceBalancePenalty(loadFuncN, thetaN)
      )
    }

    val (explicitMapping, finalMigrationCost): (Map[K, Int], Double) =
      currFreqsSorted.foldLeft((Map[K, Int](), untrackedMigrationCost)) {
        case ((explicitHash, m), (key, freq)) =>
          // best placement, initially invalid
          var j = -1
          // best utility value, lower is better
          var u = Double.MaxValue
          // old location
          val h = prevP(key)

          // for each placement
          for (l <- 0 until numPartitions) {

            val balancePenalty = Math.cbrt(
              balancePenalties.map(_.withAddedWeights(Seq(l -> freq)).penalty).product
            )

            // val balancePenalty = balancePenaltyByExplicitHash(explicitHash + (key -> l))

            val migrationPenalty = (m + betaS(freq) * indicator(l != h)) / idealMigrationCost
            val utilityOfPlacement = utility(balancePenalty, migrationPenalty)
            if (utilityOfPlacement < u) {
              j = l
              u = utilityOfPlacement
            }
          }

          balancePenalties = balancePenalties.map(_.withAddedWeights(Seq(j -> freq)))
          (explicitHash + (key -> j), m + betaS(freq) * indicator(j != h))
      }

    // println(explicitMapping)
    combineExplicitAndConsistent(explicitMapping, consistentHash)
  }

  def combineExplicitAndConsistent[K](explicitHash: Map[K, Int],
    consistentHash: K => Int): K => Int = {
    val explHash = HashMap(explicitHash.toArray: _*)

    k => explHash.getOrElse(k, consistentHash(k))
  }
}
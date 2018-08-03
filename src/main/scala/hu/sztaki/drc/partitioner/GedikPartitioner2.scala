package hu.sztaki.drc.partitioner

import com.google.common.collect.TreeMultiset

import scala.collection.JavaConversions._
import scala.collection.mutable

object GedikPartitioner2 {

  def constructPartitionerReadj[K](numPartitions: Int,
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

    def loadFuncS(freq: Double) = betaS(freq)

    def loadFuncC(freq: Double) = freq * betaC(freq)

    // the communication cost is considered linear, see paper for more details
    def loadFuncN(freq: Double) = freq

    def balancePenaltyByExplicitHash(h: Map[K, Int]): Double = {

      // inverting the map
      // TODO optimization: could probably construct an array in a more efficient way
      val inverseMap = (0 until numPartitions).map(p => (p, Iterable())).toMap ++ h.groupBy(_._2)
        .mapValues(_.keys)
      val partitions: Seq[Iterable[K]] = Seq.tabulate(numPartitions)(inverseMap)

      def resourceBalancePenalty(loadFunc: Double => Double, theta: Double): Double = {
        val keyToLoad: K => Double = x => loadFunc(currFreqsMap(x))
        val loads = partitions.map(_.toStream.map(keyToLoad).sum)

        val avgLoad = loads.sum / loads.size

        (loads.max - loads.min) / (theta * avgLoad)
      }

      Math.cbrt(
        resourceBalancePenalty(loadFuncS, thetaS) *
          resourceBalancePenalty(loadFuncC, thetaC) *
          resourceBalancePenalty(loadFuncN, thetaN)
      )
    }

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

    val indicator = (x: Boolean) => GedikPartitioner.indicator(x)

    // Balance penalty index for faster updates
    case class BalancePenaltyMutable(theta: Double,
      loadFunc: Double => Double,
      numPartitions: Int) {

      private val loadsSorted =
        TreeMultiset.create[(Int, Double)](Ordering.by[(Int, Double), Double](_._2))

      // init with 0 load at every partition
      for (i <- 0 until numPartitions) {
        loadsSorted.add((i, 0.0), 1)
      }

      private val loads: Array[Double] = Array.fill(numPartitions)(0.0)

      private var loadSum: Double = 0.0

      def penalty: Double = {
        val avgLoad = loadSum / loads.length
        val maxLoad = loadsSorted.lastEntry().getElement._2
        val minLoad = loadsSorted.firstEntry().getElement._2
        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def penaltyWithAddedWeights(ws: Seq[(Int, Double)]): Double =
        penaltyWithAddedLoads(ws.map {
          case (partition, weight) => (partition, Math.signum(weight) * loadFunc(Math.abs(weight)))
        })

      private def penaltyWithAddedLoads(loadsToAdd: Seq[(Int, Double)]): Double = {
        val invLoadsToAdd = loadsToAdd.map { case (p, l) => (p, -l) }

        loadsToAdd.foreach { case (p, l) => addLoadToPartition(p, l) }
        val tempPenalty = penalty
        invLoadsToAdd.foreach { case (p, l) => addLoadToPartition(p, l) }

        tempPenalty
      }

      private def penaltyWithAddedLoads2(loadsToAdd: Seq[(Int, Double)]): Double = {
        val loadVals = loadsToAdd.map(_._2)
        val avgLoad = (loadSum + loadVals.sum) / loads.length

        val Seq((minCandIdx, minCandDelta), (maxCandIdx, maxCandDelta)) =
          loadsToAdd.groupBy(_._1).mapValues(_.map(_._2).sum).toSeq.sortBy(_._2)

        val (currMaxLoadIdx, currMaxLoad) = loadsSorted.lastEntry().getElement
        val (currMinLoadIdx, currMinLoad) = loadsSorted.firstEntry().getElement


        val minCandidates =
          loadsSorted.iterator().toIterator.take(2).toMap ++
            Map(minCandIdx -> (loads(minCandIdx) + minCandDelta))
        val maxCandidates =
          loadsSorted.descendingMultiset().iterator().toIterator.take(2).toMap ++
            Map(maxCandIdx -> (loads(maxCandIdx) + maxCandDelta))

        val minLoad = minCandidates.values.min
        val maxLoad = maxCandidates.values.max

        (maxLoad - minLoad) / (theta * avgLoad)
      }

      def addWeightsToPartitions(ws: Seq[(Int, Double)]): Unit = ws.foreach {
        case (partition, weight) => addWeightToPartition(partition, weight)
      }

      def addWeightToPartition(partition: Int, weight: Double): Unit =
        addLoadToPartition(partition, Math.signum(weight) * loadFunc(Math.abs(weight)))

      private def addLoadToPartition(partition: Int, load: Double): Unit = {
        val prevLoad = loads(partition)
        val currLoad = prevLoad + load

        loads(partition) = currLoad

        loadsSorted.remove(partition -> prevLoad)
        loadsSorted.add(partition -> currLoad)

        loadSum += load
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
    val untrackedMigrationCost = oldFreqs.map { case (key, freq) =>
      betaS(freq) * GedikPartitioner.indicator(prevP(key) != consistentHash(key))
    }
      .sum

    // m^_ in paper
    val idealMigrationCost = allFreqs.values.map(betaS(_)).sum / numPartitions

    var balancePenaltyS = BalancePenalty.create(thetaS, loadFuncS, numPartitions)
    var balancePenaltyC = BalancePenalty.create(thetaC, loadFuncC, numPartitions)
    var balancePenaltyN = BalancePenalty.create(thetaN, loadFuncN, numPartitions)

    var balancePenalties = Seq(balancePenaltyS, balancePenaltyC, balancePenaltyN)

    // denoted by (H_p)^(t+1) in paper
    var explicitHash = currFreqsSorted.toStream.map(_._1).map(d => d -> prevP(d)).toMap
    var invExplicitHash =
      (0 until numPartitions).map((_, Set[K]())).toMap ++
        explicitHash.toSeq.groupBy(_._2).mapValues(_.map(_._1).toSet)

    val xs: Seq[Any] = null

    type Readj = Option[(Int, K, Int, Option[K])]

    // initialize balance penalties
    balancePenalties = balancePenalties.map(penalty => explicitHash.foldLeft(penalty) {
      case (prevPenalty, (key, partition)) =>
        prevPenalty.withAddedWeights(Seq(partition -> currFreqsMap(key)))
    })

    var isRunning = true
    val numReadjPerKey = new mutable.HashMap[K, Int]
    // limit number of readjustments per key
    val c = 5

    def notOverReadjLimit(d: K): Boolean = {
      numReadjPerKey.getOrElse(d, 0) <= c
    }

    var m = untrackedMigrationCost

    // last utility value
    var u = 0.0
    while (isRunning) {
      // best readjustment
      var v: Readj = None
      // best gain value
      var g = Double.MinValue

      for {
        i <- (0 until numPartitions).toStream
        j <- (0 until numPartitions).toStream
        if i != j
      } {
        for {
          d1 <- invExplicitHash(i).toStream
          d2Opt <- invExplicitHash(j).map(Some(_)).toStream :+ None
          if notOverReadjLimit(d1)
          if d2Opt.fold(true)(notOverReadjLimit)
        } {
          val d1Freq = currFreqsMap(d1)
          val addedWs = d2Opt match {
            case None =>
              Seq(i -> -d1Freq, j -> d1Freq)
            case Some(d2) =>
              val d2Freq = currFreqsMap(d2)
              Seq(i -> -d1Freq, j -> d1Freq, j -> -d2Freq, i -> d2Freq)
          }
          val penaltiesWithAdj = balancePenalties.map(_.withAddedWeights(addedWs).penalty)
          // balance penalty
          val a = Math.cbrt(penaltiesWithAdj.product)
          // worst balance
          val prevBalancePenalty = Math.cbrt(balancePenalties.map(_.penalty).product)

          if (a < prevBalancePenalty) {
            val d2MigrationPenalty = d2Opt match {
              case None => 0.0
              case Some(d2) =>
                val d2Freq = currFreqsMap(d2)
                betaS(d2Freq) * indicator(prevP(d2) == j) - betaS(d2Freq) * indicator(prevP(d2) == i)
            }
            val plusMigrationPenalty =
              betaS(d1Freq) * indicator(prevP(d1) == i) - betaS(d1Freq) * indicator(prevP(d1) == j) +
                d2MigrationPenalty
            val migrationPenalty = (m + plusMigrationPenalty) / idealMigrationCost

            // u' in paper
            val currUtility = utility(a, migrationPenalty)

            // g' in paper
            // FIXME might be - instead of + in the placement gain, like this:
            // Math.abs(currFreqsMap(d1) - d2Opt.map(currFreqsMap).getOrElse(0.0))
            val currGain = (u - currUtility) /
              Math.abs(currFreqsMap(d1) - d2Opt.map(currFreqsMap).getOrElse(0.0))

            if (currGain > g) {
              v = Some((i, d1, j, d2Opt))
              g = currGain
              u = currUtility
              ()
            } else {
              ()
            }
          }

        }
      }

      v match {
        case None =>
          isRunning = false
        case Some((i, d1, j, d2Opt)) =>
          val d1Move = Map(d1 -> j)
          val d2Move = d2Opt match {
            case None => Map()
            case Some(d2) => Map(d2 -> i)
          }
          explicitHash = explicitHash ++ d1Move ++ d2Move

          val d1Freq = currFreqsMap(d1)
          val addedWs = d2Opt match {
            case None =>
              Seq(i -> -d1Freq, j -> d1Freq)
            case Some(d2) =>
              val d2Freq = currFreqsMap(d2)
              Seq(i -> -d1Freq, j -> d1Freq, j -> -d2Freq, i -> d2Freq)
          }
          balancePenalties = balancePenalties.map(_.withAddedWeights(addedWs))

          val bPenalty = balancePenaltyByExplicitHash(explicitHash)
          val bPenaltyImmutable = Math.cbrt(balancePenalties.map(_.penalty).product)

          // updating migration cost
          val d2MigrationPenalty = d2Opt match {
            case None => 0.0
            case Some(d2) =>
              val d2Freq = currFreqsMap(d2)
              betaS(d2Freq) * indicator(prevP(d2) == j) - betaS(d2Freq) * indicator(prevP(d2) == i)
          }
          val plusMigrationPenalty =
            betaS(d1Freq) * indicator(prevP(d1) == i) - betaS(d1Freq) * indicator(prevP(d1) == j) +
              d2MigrationPenalty
          m = m + plusMigrationPenalty

          val invMapDiff = d2Opt match {
            case None => Map(
              i -> (invExplicitHash(i) - d1),
              j -> (invExplicitHash(j) + d1)
            )
            case Some(d2) => Map(
              i -> (invExplicitHash(i) - d1 + d2),
              j -> (invExplicitHash(j) - d2 + d1)
            )
          }
          invExplicitHash = invExplicitHash ++ invMapDiff

          numReadjPerKey.update(d1, numReadjPerKey.getOrElse(d1, 0) + 1)
          d2Opt.foreach(d2 => numReadjPerKey.update(d2, numReadjPerKey.getOrElse(d2, 0) + 1))
      }
    }

    GedikPartitioner.combineExplicitAndConsistent(explicitHash, consistentHash)
  }

}
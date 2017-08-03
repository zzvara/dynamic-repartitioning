package hu.sztaki.drc

import hu.sztaki.drc.partitioner.{ConsistentHashPartitioner, KeyIsolatorPartitioner, PartitioningInfo}
import hu.sztaki.drc.utilities.Distribution

import scala.collection.immutable.{HashMap, TreeSet}
import scala.util.Random

/**
	* Created by szape on 2017. 03. 30..
	*/
object KeyIsolatorPartitionerTest {


	def main(args: Array[String]): Unit = {

		runUnifiedTest()

		runNewKIPLogicTest()
	}

	def runUnifiedTest(): Unit = {
		val numPartitions: Int = 20
		val numKeys: Int = 1000
		val keys: Seq[Int] = 0 until numKeys
		val keyShuffling: Int = 4
		val shuffledKeys1: Seq[Int] = Random.shuffle[Int, Seq](keys)
		val shuffledKeys2: Seq[Int] = shuffledKeys1
			.grouped(keyShuffling).map(group => Random.shuffle[Int, Seq](group)).flatten.toSeq
		val distribution1: Array[Double] = Distribution.zeta(2, 10, numKeys).probabilities
		val distribution2: Array[Double] = Distribution.zeta(2, 8, numKeys).probabilities
		val partitioningInfo1: PartitioningInfo = PartitioningInfo
			.newInstance(shuffledKeys1.zip(distribution1), numPartitions, treeDepthHint = 10)
		val partitioningInfo2: PartitioningInfo = PartitioningInfo
			.newInstance(shuffledKeys2.zip(distribution2), numPartitions, treeDepthHint = 10)
		var partitioner2 = new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
			(pi, weighting) => new ConsistentHashPartitioner(weighting, 100))
		partitioner2 = partitioner2.update(partitioningInfo1)
		partitioner2 = partitioner2.update(partitioningInfo2)
		println("Internal migration cost estimation with ConsistentHashPartitioner: " +
			s"${partitioner2.getInternalMigrationCostEstimation}")
		println("Total migration cost estimation with ConsistentHashPartitioner: " +
			s"${partitioner2.getMigrationCostEstimation}")
	}

	def runNewKIPLogicTest(): Unit = {

		val numPartitions = 4
		val heavyKeys = Seq[(Any, Double)](("B", 0.182d), ("C", 0.164d), ("A", 0.145d), ("H", 0.127d),
			("K", 0.109d), ("I", 0.091d), ("E", 0.073d), ("D", 0.055d))

		def getPartition(k: Any): Int = {
			k match {
				case k: String =>
					k match {
						case "A" => 0
						case "B" => 1
						case "C" => 2
						case "D" => 3
						case "E" => 1
						case "F" => 2
						case "G" => 3
						case "H" => 2
						case "I" => getInternalPartition("I")
						case "J" => getInternalPartition("J")
						case "K" => getInternalPartition("K")
						case "L" => getInternalPartition("L")
					}
				case _ => throw new RuntimeException("Only String keys are supported")
			}
		}

		def getInternalPartition(k: Any): Int = {
			k match {
				case k: String =>
					k match {
						case "A" => 1
						case "B" => 0
						case "C" => 3
						case "D" => 2
						case "E" => 1
						case "F" => 3
						case "G" => 1
						case "H" => 3
						case "I" => 0
						case "J" => 2
						case "K" => 0
						case "L" => 2
					}
				case _ => throw new RuntimeException("Only String keys are supported")
			}
		}

		update(PartitioningInfo.newInstance(heavyKeys, 4, 10))

		def update(partitioningInfo: PartitioningInfo): Unit = {

			implicit val ordering = new Ordering[(Int, Double)] {
				override def compare(x: (Int, Double), y: (Int, Double)): Int = {
					val res0 = x._2.compareTo(y._2)
					if (res0 != 0) {
						res0
					} else {
						x._1.compareTo(y._1)
					}
				}
			}

			val keyExcess = 1 * numPartitions
			val allowedBalanceError = 0.0d

			val numHeavyKeys = Math.min(numPartitions + keyExcess, partitioningInfo.heavyKeys.size)
			// ordered by frequency
			val heavyKeysWithFrequencies = partitioningInfo.heavyKeys.take(numHeavyKeys)
			val heavyKeyToFrequencyMap: Map[Any, Double] = heavyKeysWithFrequencies.toMap
			val allowedLevel = Math.max(heavyKeysWithFrequencies.head._2, 1.0d / numPartitions) +
				allowedBalanceError
			var explicitHash = Map[Any, Int]()

			var partitionSizes: TreeSet[(Int, Double)] = new TreeSet[(Int, Double)]()
			var partitionToSizeMap = new HashMap[Int, Double]
			(0 until numPartitions).foreach(p => {
				partitionSizes += ((p, 0.0d))
				partitionToSizeMap += (p -> 0.0d)
			})
			var heavyKeysToMigrate: Seq[Any] = Seq[Any]()

			assert(numHeavyKeys == 8)

			val heavyKeys: Seq[Any] = heavyKeysWithFrequencies.map(_._1)
			heavyKeys.foreach { k =>
				val oldPartition = getPartition(k)
				val partitionSize = partitionToSizeMap(oldPartition) + heavyKeyToFrequencyMap(k)
				if (partitionSize < allowedLevel) {
					explicitHash += (k -> oldPartition)
					// updating bookkeep
					partitionSizes -= ((oldPartition, partitionToSizeMap(oldPartition)))
					partitionSizes += ((oldPartition, partitionSize))
					partitionToSizeMap += (oldPartition -> partitionSize)
				} else {
					heavyKeysToMigrate = heavyKeysToMigrate :+ k
				}
			}

			assert(partitionSizes == Set[(Int, Double)]((0, 0.236), (1, 0.182), (2, 0.164), (3, 0.055)))
			assert(partitionToSizeMap == Map[Int, Double](0 -> 0.236, 1 -> 0.182, 2 -> 0.164, 3 -> 0.055))
			assert(explicitHash == Map[Any, Int]("A" -> 0, "B" -> 1, "C" -> 2, "D" -> 3, "I" -> 0))
			assert(heavyKeysToMigrate == Seq[Any]("H", "K", "E"))

			heavyKeysToMigrate.foreach { k =>
				val consistentHashPartition: Int = getInternalPartition(k)
				if (partitionToSizeMap(consistentHashPartition) + heavyKeyToFrequencyMap(k) <
					allowedLevel) {
					val partitionSize =
						partitionToSizeMap(consistentHashPartition) + heavyKeyToFrequencyMap(k)
					explicitHash += (k -> consistentHashPartition)
					partitionSizes -= ((consistentHashPartition, partitionToSizeMap(consistentHashPartition)))
					partitionSizes += ((consistentHashPartition, partitionSize))
					partitionToSizeMap += (consistentHashPartition -> partitionSize)
				} else {
					val (p, v) = partitionSizes.min
					explicitHash += (k -> p)
					val partitionSize = v + heavyKeyToFrequencyMap(k)
					partitionSizes -= ((p, v))
					partitionSizes += ((p, partitionSize))
					partitionToSizeMap += (p -> partitionSize)
				}
			}

			assert(partitionSizes == Set[(Int, Double)]((0, 0.236), (1, 0.255), (2, 0.273), (3, 0.182)))
			assert(partitionToSizeMap == Map[Int, Double](0 -> 0.236, 1 -> 0.255, 2 -> 0.273, 3 -> 0.182))
			assert(explicitHash == Map[Any, Int]("A" -> 0, "B" -> 1, "C" -> 2, "D" -> 3, "E" -> 1,
				"H" -> 3, "I" -> 0, "K" -> 2))

			// recalculating weighting for the consistent hash partitioner
			val level: Double =
				PartitioningInfo.newInstance(partitionToSizeMap.toSeq, numPartitions, 31).level
			val unnormalizedWeighting: Array[Double] = (0 until numPartitions).map { p =>
				Math.max(0.0d, level - partitionToSizeMap(p))
			}.toArray
			val normalizationFactor: Double = unnormalizedWeighting.sum
			val weighting: Array[Double] = unnormalizedWeighting.map(_ / normalizationFactor)

			assert(level == 0.25)
			assert(unnormalizedWeighting.map(w =>
				BigDecimal(w).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble) sameElements
				Array[Double](0.014, 0.0, 0.0, 0.068))
			assert(weighting.map(w =>
				BigDecimal(w).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble) sameElements
				Array[Double](0.171, 0.0, 0.0, 0.829))
		}
	}
}

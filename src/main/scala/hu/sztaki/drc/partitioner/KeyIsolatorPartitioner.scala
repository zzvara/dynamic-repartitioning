package hu.sztaki.drc.partitioner

import scala.collection.immutable.{HashMap, TreeSet}

object KeyIsolatorPartitioner {

	implicit object Factory extends PartitionerFactory {
		override def apply(numPartitions: Int): KeyIsolatorPartitioner[ConsistentHashPartitioner] = {
			new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
				(pi, weighting) => new ConsistentHashPartitioner(weighting, 100))
		}
	}
}

class KeyIsolatorPartitioner[T <: Partitioner
	with MigrationCostEstimator with Adaptive[T]] private(
	val numPartitions: Int,
	val heavyKeysMap: Map[Any, Int] = Map[Any, Int](),
	val internalPartitioner: Partitioner,
	val migrationCostEstimation: Option[(Double, Double)] = None,
	val initializeInternalPartitioner: (PartitioningInfo, Array[Double]) => T)
	extends Updateable with MigrationCostEstimator {

	def this(numPartitions: Int,
		initializeInternalPartitioner: (PartitioningInfo, Array[Double]) => T) = {
		this(numPartitions,
			internalPartitioner = new HashPartitioner(numPartitions),
			initializeInternalPartitioner = initializeInternalPartitioner)
	}

	override def get(key: Any): Int = {
		heavyKeysMap.get(key) match {
			case Some(part) => part
			case None =>
				internalPartitioner.get(key)
		}
	}

	override val size: Int = numPartitions

	override def getMigrationCostEstimation: Option[Double] =
		migrationCostEstimation.map({ case (hm, sm) => hm + sm })

	def getInternalMigrationCostEstimation: Option[Double] =
		migrationCostEstimation.map({ case (hm, sm) => sm })

	internalPartitioner match {
		case p: MigrationCostEstimator => p.getMigrationCostEstimation
		case _ => None
	}

	override def update(partitioningInfo: PartitioningInfo): KeyIsolatorPartitioner[T] = {

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
		val allowedBalanceError = 0.001d

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

		val heavyKeys: Seq[Any] = heavyKeysWithFrequencies.map(_._1)
		heavyKeys.foreach { k =>
			val oldPartition = get(k)
			val partitionSize = partitionToSizeMap(oldPartition) + heavyKeyToFrequencyMap(k)
			if (partitionSize <= allowedLevel) {
				explicitHash += (k -> oldPartition)
				// updating bookkeep
				partitionSizes -= ((oldPartition, partitionToSizeMap(oldPartition)))
				partitionSizes += ((oldPartition, partitionSize))
				partitionToSizeMap += (oldPartition -> partitionSize)
			} else {
				heavyKeysToMigrate = heavyKeysToMigrate :+ k
			}
		}

		heavyKeysToMigrate.foreach { k =>
			val consistentHashPartition: Int = internalPartitioner.get(k)
			if (partitionToSizeMap(consistentHashPartition) + heavyKeyToFrequencyMap(k) < allowedLevel) {
				val partitionSize = partitionToSizeMap(consistentHashPartition) + heavyKeyToFrequencyMap(k)
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

		// recalculating partitioning info from partitionSizes
		val level: Double = PartitioningInfo.newInstance(partitionToSizeMap.toSeq,
			numPartitions, 31).level
		val unnormalizedWeighting: Array[Double] = (0 until numPartitions).map { p =>
			Math.max(0.0d, level - partitionToSizeMap(p))
		}.toArray
		val normalizationFactor: Double = unnormalizedWeighting.sum
		val weighting: Array[Double] = unnormalizedWeighting.map(_ / normalizationFactor)

		// updating consistent hash partitioner
		val consistentHashPartitioner: T = internalPartitioner match {
			case p: Partitioner with Adaptive[T] =>
				p.adapt(partitioningInfo, weighting)
			case _ => initializeInternalPartitioner(partitioningInfo, weighting)
		}

		val migrationCostEstimation: Option[(Double, Double)] =
			consistentHashPartitioner.getMigrationCostEstimation match {
				case Some(c) =>
					val numHeavyKeysOut = (heavyKeysMap.keySet -- heavyKeyToFrequencyMap.keySet)
						.count(k => heavyKeysMap(k) != consistentHashPartitioner.get(k))
					val heavyKeyMigration = heavyKeysToMigrate.map(heavyKeyToFrequencyMap).sum +
						numHeavyKeysOut * heavyKeysWithFrequencies.last._2
					val fractionOfSmallKeys = 1.0d - heavyKeyToFrequencyMap.values.sum
					val smallKeysMigration = fractionOfSmallKeys * c
					Some((heavyKeyMigration, smallKeysMigration))
				case None => None
			}

		new KeyIsolatorPartitioner(numPartitions,
			explicitHash,
			consistentHashPartitioner,
			migrationCostEstimation,
			initializeInternalPartitioner)
	}
}
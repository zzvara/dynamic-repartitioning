package hu.sztaki.drc.partitioner

import scala.util.hashing.MurmurHash3

/**
	* Created by szape on 2017. 03. 21..
	*/
class ConsistentHashPartitioner private(
	val numPartitions: Int,
	val replicationFactor: Int,
	val weighting: Array[Double],
	val hashRing: Array[Int],
	val migrationCostEstimation: Option[Double],
	hashFunction: Any => Double) extends Partitioner with MigrationCostEstimator
	with Adaptive[ConsistentHashPartitioner] {
	// TODO increase number of partitions with the number of hosts unchanged
	// TODO construct with numReplicas instead of replicationFactor

	private def this(
		numPartitions: Int,
		replicationFactor: Int,
		weighting: Array[Double],
		initialPoints: Array[Int],
		hashFunction: Any => Double) {
		this(numPartitions,
			replicationFactor,
			weighting,
			ConsistentHashPartitioner.createInitialHashRing(numPartitions, initialPoints),
			None,
			hashFunction)
	}

	def this(
		weighting: Array[Double],
		replicationFactor: Int = 100,
		hashFunction: Any => Double = key =>
			(MurmurHash3.stringHash((key.hashCode + 123456791).toString).toDouble /
				Int.MaxValue + 1) / 2) {
		this(weighting.length,
			replicationFactor,
			weighting,
			ConsistentHashPartitioner
				.createInitialPoints(weighting.length * replicationFactor, weighting),
			hashFunction)
	}

	override val size: Int = numPartitions

	private val numPackages = numPartitions * replicationFactor

	override def get(key: Any): Int = {
		hashRing(Math.floor(hashFunction(key) * numPackages).toInt % numPackages)
	}

	override def getMigrationCostEstimation: Option[Double] = migrationCostEstimation

	override def adapt(partitioningInfo: PartitioningInfo,
		newWeighting: Array[Double]): ConsistentHashPartitioner = {
		val newHashRing: Array[Int] = new Array[Int](numPackages)
		val transfer: Array[Int] = weighting.zip(newWeighting).map(p => p._2 - p._1)
			.scan(0.0d)(_ + _)
			.map(d => Math.round(d * numPackages).toInt) // assert last = 0
			.sliding(2).map(p => p(1) - p(0)).toArray
		val migrationCostEstimation = transfer.filter(t => t > 0).sum.toDouble / numPackages
		var transferTo: Int = 0
		while (transferTo < transfer.length && transfer(transferTo) <= 0) {
			transferTo += 1
		}
		for (replica <- 0 until numPackages) {
			val oldPartition = hashRing(replica)
			if (transferTo < transfer.length && transfer(oldPartition) < 0) {
				newHashRing(replica) = transferTo
				// Updating transfer
				transfer(oldPartition) += 1
				transfer(transferTo) -= 1
				// Updating transferTo
				while (transferTo < transfer.length && transfer(transferTo) <= 0) {
					transferTo += 1
				}
			} else {
				newHashRing(replica) = oldPartition
			}
		}

		new ConsistentHashPartitioner(
			numPartitions,
			replicationFactor,
			newWeighting,
			newHashRing,
			Some(migrationCostEstimation),
			hashFunction)
	}

	// for testing
	def printHashRing(): Unit = {
		println("Hash ring: " + hashRing.mkString("[", ", ", "]"))
	}
}

object ConsistentHashPartitioner {

	def createInitialPoints(numPackages: Int, weighting: Array[Double]): Array[Int] = {
		// TODO add normalization correction
		weighting.scan(0.0d)(_ + _).map(w => Math.rint(w * numPackages).toInt)
	}

	def createInitialHashRing(numPartitions: Int, initialPoints: Array[Int]): Array[Int] = {
		val hashRing: Array[Int] = new Array[Int](initialPoints.last)
		(0 until numPartitions).foreach { part =>
			(initialPoints(part) until initialPoints(part + 1)).foreach { host =>
				hashRing(host) = part
			}
		}
		hashRing
	}
}

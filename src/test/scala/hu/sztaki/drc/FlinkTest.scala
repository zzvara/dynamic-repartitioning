package hu.sztaki.drc

import hu.sztaki.drc.partitioner.{ConsistentHashPartitioner, KeyIsolatorPartitioner, PartitioningInfo}
import hu.sztaki.drc.utilities.Distribution

object FlinkTest {

	def main(args: Array[String]): Unit = {
		flinkTest()
	}

	def flinkTest(): Unit = {
		val numPartitions: Int = 50
		val numKeys: Int = 1000000
		//		val partitions: Seq[Int] = 0 until numPartitions
		val keys: Seq[Int] = 0 until numKeys
		val numHeavyKeys = 2 * numPartitions // from default keyExcess
		val heavyKeys = 0 until numHeavyKeys

		val distribution: Array[Double] = Distribution.zeta(1.5, 5, numKeys).probabilities
		val heavyKeyHistogram = heavyKeys.map(k => (k, distribution(k)))
		val globalHistogram = keys.zip(distribution)
		val partitioningInfo: PartitioningInfo = PartitioningInfo.newInstance(globalHistogram, numPartitions, 31)
		val partitioner = new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
			(_, weighting) => new ConsistentHashPartitioner(weighting, 100)).update(partitioningInfo)
		val partitionSizes = globalHistogram.map({ case (k, f) => (partitioner.get(k), f) })
			.groupBy(_._1).map(g => (g._1, g._2.map(_._2).sum)).toArray.sortBy(_._1)

		println(s"Heavy keys: ${heavyKeyHistogram.take(20).mkString("[", ", ", "...]")}")
		println(s"Partitioning info: $partitioningInfo")
		println(s"Heavy keys to partitions: ${
			heavyKeys.map(k => (k, partitioner.get(k)))
				.mkString("[", ", ", "...]")
		}")
		println(s"Partition sizes: ${partitionSizes.mkString("[", ", ", "]")}")
		println(s"Ordered partition sizes: ${partitionSizes.sortBy(-_._2).mkString("[", ", ", "]")}")
	}
}
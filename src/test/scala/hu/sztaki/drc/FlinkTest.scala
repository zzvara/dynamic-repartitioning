package hu.sztaki.drc

import hu.sztaki.drc.partitioner.{ConsistentHashPartitioner, KeyIsolatorPartitioner, PartitioningInfo}
import hu.sztaki.drc.utilities.Distribution

import scala.util.hashing.MurmurHash3

object FlinkTest {
	def main(args: Array[String]): Unit = {
		flinkTest()
		hashTest()
		dirftRespectingTest()
	}

	def hashTest(): Unit = {
		(0 to 100).map(_.toString).map {
			x => MurmurHash3.stringHash(x, -1849321689).hashCode().toString
		}.zip {
			(0 to 100).map(_.toString).map {
				x => MurmurHash3.stringHash(x, -1849321689).hashCode().toString
			}
		}.map {
			case (s1, s2) =>
				s1 == s2
		}.foreach {
			println
		}
	}

	def dirftRespectingTest(): Unit = {
		val numPartitions: Int = 42
		val numKeys: Int = 1000000
		val distribution: Array[Double] = Distribution.zeta(1.0, 50, numKeys).probabilities

		val sampler = new DefaultDriftRespecting

		(1 to 50000).foreach {
			i =>
				sampler.add(MurmurHash3.stringHash(i.toString, -42819321).toString)
		}

		sampler.getValue.toSeq.sortBy(-_._2).take(100).foreach {
			println
		}
	}

	def flinkTest(): Unit = {
		val numPartitions: Int = 42
		val numKeys: Int = 1000000
		//		val partitions: Seq[Int] = 0 until numPartitions
		val keys: Seq[Int] = 0 until numKeys
		val numHeavyKeys = 2 * numPartitions // from default keyExcess
		val heavyKeys = 0 until numHeavyKeys

		val distribution: Array[Double] = Distribution.zeta(1.0, 50, numKeys).probabilities
		val heavyKeyHistogram = heavyKeys.map(k => (k, distribution(k)))
		val globalHistogram = keys.zip(distribution)
		val partitioningInfo: PartitioningInfo = PartitioningInfo.newInstance(globalHistogram, numPartitions, 31)
		val partitioner = new KeyIsolatorPartitioner[ConsistentHashPartitioner](numPartitions,
			(_, weighting) => new ConsistentHashPartitioner(weighting, 100)).update(partitioningInfo)
		val partitionSizes = globalHistogram.map({ case (k, f) => (partitioner.get(k), f) })
			.groupBy(_._1).map(g => (g._1, g._2.map(_._2).sum)).toArray.sortBy(_._1)

		println(s"Heavy keys: ${heavyKeyHistogram.take(20).mkString("[", ", ", "...]")}")
		// println(s"Partitioning info: $partitioningInfo")
		println(s"Heavy keys to partitions: ${
			heavyKeys.map(k => (k, partitioner.get(k)))
				.mkString("[", ", ", "...]")
		}")
		println(s"Partition sizes: ${partitionSizes.mkString("[", ", ", "]")}")
		println(s"Ordered partition sizes: ${partitionSizes.sortBy(-_._2).mkString("[", ", ", "]")}")
	}
}
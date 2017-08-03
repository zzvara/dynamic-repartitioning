package hu.sztaki.drc

import hu.sztaki.drc.partitioner.ConsistentHashPartitioner
import hu.sztaki.drc.utilities.Distribution

/**
	* Created by szape on 2017. 03. 21..
	*/
object ConsistentHashPartitionerTest {

	def main(args: Array[String]): Unit = {
		val numPartitions: Int = 20
		val weighting: Array[Double] =
			Distribution.twoStep(numPartitions.toDouble / 3.0d, numPartitions).probabilities
		val newWeighting: Array[Double] =
			Distribution.twoStep(numPartitions.toDouble / 2.0d, numPartitions).probabilities
		val keys: Seq[String] = Seq("alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta",
			"theta", "iota", "kappa", "lambda", "mu")

		var partitioner: ConsistentHashPartitioner = new ConsistentHashPartitioner(weighting, 100)
		partitioner.printHashRing()
		// TODO assert
		keys.foreach { key =>
			println(s"$key -> ${partitioner.get(key)}")
		}
		println(s"migrationCostEstimation: ${partitioner.migrationCostEstimation}")

		partitioner = partitioner.adapt(null, newWeighting)
		partitioner.printHashRing()
		keys.foreach { key =>
			println(s"$key -> ${partitioner.get(key)}")
		}
		println(s"migrationCostEstimation: ${partitioner.migrationCostEstimation}")
	}
}

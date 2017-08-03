package hu.sztaki.drc.partitioner

trait Adaptive[T <: Partitioner with Adaptive[T]] {

	def adapt(partitioningInfo: PartitioningInfo, newWeighting: Array[Double]): T
}

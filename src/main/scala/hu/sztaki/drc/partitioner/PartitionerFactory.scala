package hu.sztaki.drc.partitioner

trait PartitionerFactory {

	def apply(numPartitions: Int): Partitioner with Updateable
}

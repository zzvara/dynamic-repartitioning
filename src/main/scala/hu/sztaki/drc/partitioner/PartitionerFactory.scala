package hu.sztaki.drc.partitioner

trait PartitionerFactory extends Serializable {
	def apply(numPartitions: Int): Partitioner with Updateable
}
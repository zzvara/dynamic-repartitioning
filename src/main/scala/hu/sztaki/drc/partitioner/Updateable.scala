package hu.sztaki.drc.partitioner

trait Updateable extends Partitioner {

  def update(partitioningInfo: PartitioningInfo): Updateable

}
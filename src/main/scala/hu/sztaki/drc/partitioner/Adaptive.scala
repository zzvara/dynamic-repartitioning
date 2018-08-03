package hu.sztaki.drc.partitioner

trait Adaptive[P <: Adaptive[P]] extends Partitioner {

  def adapt(partitioningInfo: PartitioningInfo, newWeighting: Array[Double]): P

}
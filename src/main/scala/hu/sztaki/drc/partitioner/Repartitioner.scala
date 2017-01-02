package hu.sztaki.drc.partitioner

abstract class Repartitioner(val parent: Partitioner) extends Partitioner {
  def get(key: Any, oldPartition: Int): Int
}

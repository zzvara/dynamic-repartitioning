package hu.sztaki.drc.partitioner

abstract class Partitioner extends Serializable {
  def size: Int
  def get(key: Any): Int
}

package hu.sztaki.drc.partitioner

import hu.sztaki.drc.utilities.StringIdGenerator

abstract class Partitioner extends Serializable {
  def size: Int
  def get(key: Any): Int
  val id = StringIdGenerator.generate()
}

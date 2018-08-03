package hu.sztaki.drc.partitioner

import scala.collection.immutable.TreeMap

object SortedTree {

  def emptyPartitions(numPartitions: Int) =
    new SortedTree(TreeMap(0.0 -> (0 until numPartitions).toSet))

}

case class SortedTree(tree: TreeMap[Double, Set[Int]] = new TreeMap[Double, Set[Int]]()) {

  def min: (Int, Double) = {
    val (load, partitions) = tree.last
    (partitions.head, load)
  }

  def max: (Int, Double) = {
    val (load, partitions) = tree.head
    (partitions.head, load)
  }

  def +(elem: (Int, Double)): SortedTree = {
    val (partition, load) = elem
    val partitions = tree.getOrElse(load, Set()) + partition
    this.copy(tree - load + (load -> partitions))
  }

  def -(elem: (Int, Double)): SortedTree = {
    val (partition, load) = elem
    val partitions = tree(load) - partition

    val nextTree =
      if (partitions.isEmpty) {
        tree - load
      } else {
        tree - load + (load -> partitions)
      }
    this.copy(tree = nextTree)
  }
}
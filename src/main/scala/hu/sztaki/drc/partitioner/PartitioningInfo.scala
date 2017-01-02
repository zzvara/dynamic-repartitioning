package hu.sztaki.drc.partitioner

import scala.annotation.tailrec

case class PartitioningInfo(
  partitions: Int,
  cut: Int,
  sCut: Int,
  level: Double,
  sortedKeys: Array[Any],
  sortedValues: Array[Double]) {
  override def toString: String = {
    s"PartitioningInfo [numberOfPartitions=$partitions, cut=$cut, sCut=$sCut, " +
    s"level=$level]"
  }
}

object PartitioningInfo {
  def newInstance(globalHistogram: scala.collection.Seq[(Any, Double)], numPartitions: Int,
                  treeDepthHint: Int, sCutHint: Int = 0): PartitioningInfo = {
    require(numPartitions > 0, "Where's my number of partitions, I can not call you maybe!")

    val sortedValues = globalHistogram.map(_._2).toArray.take(numPartitions)
    val sortedKeys = globalHistogram.map(_._1).toArray.take(numPartitions)
    val pCutHint = Math.pow(2, treeDepthHint - 1).toInt
    val startingCut = Math.min(numPartitions, sortedValues.length)
    var computedSCut = 0
    var computedLevel = 1.0d / numPartitions
    var remainder = 1.0d

    @tailrec
    def computeCuts(i: Int): Unit = {
      if (i < startingCut && computedLevel <= sortedValues(i)) {
        remainder -= sortedValues(i)
        if (i < numPartitions - 1) computedLevel = remainder / (numPartitions - 1 - i) else computedLevel = 0.0d
        computedSCut += 1
        computeCuts(i + 1)
      }
    }
    computeCuts(0)

    val actualSCut = Math.max(sCutHint, computedSCut)
    val actualPCut = Math.min(pCutHint, startingCut - actualSCut)
    /**
      * Recompute level to minimize rounding errors.
      */
    val level = Math.max(0, (1.0d - sortedValues.take(actualSCut).sum) / (numPartitions - actualSCut))
    val actualCut = actualSCut + actualPCut

    new PartitioningInfo(numPartitions, actualCut, actualSCut, level, sortedKeys, sortedValues)
  }
}

package hu.sztaki.drc.partitioner

class WeightedHashPartitioner(
  weights: Array[Double],
  partitioningInfo: PartitioningInfo,
  hash: Any => Double) extends Partitioner {
  private val partitions = partitioningInfo.partitions
  private val cut = partitioningInfo.cut
  private val sCut = partitioningInfo.sCut
  private val level = partitioningInfo.level
  private val block = (partitions - cut) * level
  assert(weights.length == cut - sCut)

  private val precision = size / 1.0e9
  for (i <- 0 until cut - sCut - 1 by 1) {
    assert(weights(i) >= weights(i + 1) - precision)
  }
  assert(weights.isEmpty || weights.last >= 0 - precision, s"${weights.mkString("[", ",", "]")}")
  private val aggregated = weights.scan(0.0d)(_ + _).drop(1)
  private val sum = if (cut > sCut) aggregated.last else 0
  private val searchTree = new BalancedTree(aggregated)

  override def size: Int = partitions - partitioningInfo.sCut

  override def get(key: Any): Int = {
    val searchKey = hash(key) * (block + sum)
    val bucket = if (searchKey == 0.0d) {
      0
    } else if (searchKey <= block) {
      (searchKey / level).ceil.toInt - 1
    } else {
//      BinarySearch.binarySearch(aggregated, searchKey - block) + partitions - cut
      // new search algorithm, faster than binary search
      searchTree.getPartition(searchKey - block) + partitions - cut
    }
    bucket
  }
}

object WeightedHashPartitioner {
  def newInstance(partitioningInfo: PartitioningInfo,
                  hash: Any => Double): WeightedHashPartitioner = {
    val cut = partitioningInfo.cut
    val sortedValues: Array[Double] = partitioningInfo.sortedValues

    new WeightedHashPartitioner(
      Array.tabulate[Double](cut - partitioningInfo.sCut)
        (i => partitioningInfo.level - sortedValues(cut - i - 1)),
      partitioningInfo,
      hash)
  }
}

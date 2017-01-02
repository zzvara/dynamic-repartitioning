package hu.sztaki.drc.partitioner

class KeyIsolationPartitioner(
  val partitioningInfo: PartitioningInfo,
  weightedHashPartitioner: WeightedHashPartitioner) extends Partitioner {

  val sortedKeys: Array[Any] = partitioningInfo.sortedKeys
  val heaviestKeys = sortedKeys.take(partitioningInfo.cut)
  private val heavyKeysMap = Map[Any, Int]() ++ heaviestKeys.zipWithIndex

  override def size: Int = partitioningInfo.partitions

  override def get(key: Any): Int = {
    heavyKeysMap.get(key) match {
      case Some(k) => k
      case None =>
        val bucket = partitioningInfo.partitions - weightedHashPartitioner.get(key) - 1
        bucket
    }
  }
}

package hu.sztaki.drc.partitioner

class HashPartitioner(override val numPartitions: Int) extends Partitioner {
  require(numPartitions >= 0, s"Number of partitions ($numPartitions) should be non-negative.")

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ =>
      val rawMod = key.hashCode % numPartitions
      rawMod + (if (rawMod < 0) numPartitions else 0)
  }

  override def hashCode(): Int = numPartitions

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner => h.numPartitions == numPartitions
    case _ => false
  }

  override def toString: String = s"HashPartitioner($numPartitions)"

}
package hu.sztaki.drc.partitioner

/**
  * Created by szape on 2016.05.03..
  * Copied from Apache Spark
  */
class HashPartitioner(numPartitions: Int) extends Partitioner {
  require(numPartitions >= 0, s"Number of partitions ($numPartitions) cannot be negative.")

  def size: Int = numPartitions

  def get(key: Any): Int = key match {
    case null => 0
    case _ =>
      val rawMod = key.hashCode % numPartitions
      rawMod + (if (rawMod < 0) numPartitions else 0)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.size == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

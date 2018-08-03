package hu.sztaki.drc.partitioner

trait Partitioner extends Serializable {

  val id: String = StringGenerator.generateId()

  def numPartitions: Int

  def getPartition(key: Any): Int

  override def hashCode(): Int = id.hashCode()

  override def equals(other: Any): Boolean = other match {
    case p: Partitioner => p.id == id
    case _ => false
  }

  override def toString: String = s"Partitioner($id)"

}
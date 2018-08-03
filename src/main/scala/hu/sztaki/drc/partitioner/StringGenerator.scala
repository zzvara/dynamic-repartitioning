package hu.sztaki.drc.partitioner

import scala.util.Random

object StringGenerator {
  val idLength: Int = 10
  val alphNumList: Seq[Char] = ((48 to 57) ++ (65 to 90) ++ (95 to 95) ++ (97 to 122)).map(_.toChar)

  def generateId(): String = {
    (1 to idLength).map(_ => {
      val hashed = Random.nextInt(alphNumList.size)
      alphNumList(hashed)
    }).mkString
  }
}
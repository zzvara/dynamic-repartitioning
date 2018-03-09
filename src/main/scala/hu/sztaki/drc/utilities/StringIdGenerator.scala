package hu.sztaki.drc.utilities

import scala.util.Random

object StringIdGenerator extends Generator[String] {
	val length = 10
	val alphNumList = ((48 to 57) ++ (65 to 90) ++ (95 to 95) ++ (97 to 122)).map(_.toChar)

	override def generate(): String = {
		(1 to length).map(_ => {
			val hashed = Random.nextInt(alphNumList.size)
			alphNumList(hashed)
		}).mkString
	}
}

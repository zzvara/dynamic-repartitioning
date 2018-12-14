package hu.sztaki.drc.sampler

import hu.sztaki.drc.utilities.Distribution

import scala.util.Random

object SamplerExample {

	def main(args: Array[String]): Unit = {
		val rnd = new Random(736452938)
		val numKeys = 100
		val numRecords = 10000
		val distribution: Distribution = Distribution.linear(numKeys, numKeys)
		val data: Seq[(Any, Double)] = (1 to numRecords).map(_ => (distribution.sample(), rnd.nextDouble()))

		val baseSampler = new BaseSampler()
		data.foreach(baseSampler.add)

		val histogram = baseSampler.value

		println(baseSampler.map)
		println(baseSampler.externalLoad)
		println(baseSampler.internalLoad)
		println(baseSampler.samplingRate)
		println(baseSampler.scale)
		println(baseSampler.version)
		println(baseSampler.width)
		println(baseSampler.isEmpty)
		println(baseSampler.isNormed)
		println(baseSampler.value.toArray.sortBy(_._2).mkString)

		baseSampler.scaleDown(2)
		baseSampler.scale = 0.5d

		data.foreach(baseSampler.add)

		baseSampler.cutBack(50)

		println(baseSampler.map)
		println(baseSampler.externalLoad)
		println(baseSampler.internalLoad)
		println(baseSampler.samplingRate)
		println(baseSampler.scale)
		println(baseSampler.version)
		println(baseSampler.width)
		println(baseSampler.isEmpty)
		println(baseSampler.isNormed)
		println(baseSampler.value.toArray.sortBy(_._2).mkString)

		baseSampler.scaleDown(baseSampler.internalLoad)
		println(baseSampler.isNormed)

//		baseSampler.setHistogram(histogram)

//		println(baseSampler.map)
//		println(baseSampler.externalLoad)
//		println(baseSampler.internalLoad)
//		println(baseSampler.samplingRate)
//		println(baseSampler.scale)
//		println(baseSampler.version)
//		println(baseSampler.width)
//		println(baseSampler.isEmpty)
//		println(baseSampler.isNormed)
//		println(baseSampler.value.toArray.sortBy(_._2).mkString)

//		baseSampler.reset()
//		println(baseSampler.isEmpty)
	}
}

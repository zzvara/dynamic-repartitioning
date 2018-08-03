package hu.sztaki.drc.utilities

import scala.util.Random

class Distribution(val probabilities: Array[Double]) extends Serializable {

	import Distribution._

	val width = probabilities.length
	val precision = width.toDouble / 1000000

	for (i <- 0 until width - 1) {
		assert(probabilities(i) >= probabilities(i + 1) - precision)
	}
	assert(probabilities.isEmpty || probabilities.last >= 0 - precision)

	val aggregated = probabilities.scan(0.0d)(_ + _).drop(1)
	assert(Math.abs(aggregated.last - 1) <= precision)

	def get(index: Int): Double = probabilities(index - 1)

	def sample(): Int = {
		val rnd = 1 - Random.nextDouble()
		binarySearch(aggregated, rnd) + 1
	}

	def sample(x: Double): Int = {
		assert(x >= 0 && x <= 1)
		binarySearch(aggregated, x) + 1
	}

	def empiric(sampleSize: Int): Distribution = {
		val empiric = Array.fill[Double](width)(0.0d)
		for (i <- 1 to sampleSize) {
			empiric(sample() - 1) += 1.0d
		}
		for (i <- 0 until width) {
			empiric(i) /= sampleSize
		}
		new Distribution(empiric.sortBy(-_))
	}

	def unorderedEmpiric(sampleSize: Int): Array[Double] = {
		val empiric = Array.fill[Double](width)(0.0d)
		for (i <- 1 to sampleSize) {
			empiric(sample() - 1) += 1.0d
		}
		for (i <- 0 until width) {
			empiric(i) /= sampleSize
		}
		empiric
	}

	override def toString: String = {
		s"Distribution${probabilities.mkString("(", ", ", ")")}"
	}
}

object Distribution {

	def binarySearch(array: Array[Double], value: Double): Int = {
		binarySearch((i: Int) => array(i), value, 0, array.length - 1)
	}

	def binarySearch(f: Int => Double, value: Double, lower: Int, upper: Int): Int = {
		if (lower == upper) {
			lower
		} else {
			val middle = (lower + upper) / 2
			if (value <= f(middle)) {
				binarySearch(f, value, lower, middle)
			} else {
				binarySearch(f, value, middle + 1, upper)
			}
		}
	}

	def exponential(lambda: Double, width: Int): Distribution = {
		assert(lambda >= 0.0d && lambda <= 1.0d)
		assert(width >= 1)
		val normalizer = if (lambda < 1) (1.0d - Math.pow(lambda, width)) / (1.0d - lambda) else width
		val probabilities = Array.tabulate[Double](width)(i => Math.pow(lambda, i) / normalizer)
		new Distribution(probabilities)
	}

	def uniform(width: Int): Distribution = {
		assert(width >= 1)
		val probabilities = Array.fill[Double](width)(1.0d / width)
		new Distribution(probabilities)
	}

	def dirac(width: Int): Distribution = {
		assert(width >= 1)
		val probabilities = Array.tabulate[Double](width)(i => if (i == 0) 1 else 0)
		new Distribution(probabilities)
	}

	def linear(spread: Int, width: Int): Distribution = {
		assert(width >= 1 && spread >= 1)
		val normalizer =
			if (spread >= width) (width * (2 * spread - width + 1)).toDouble / 2
			else (spread * (spread + 1)).toDouble / 2
		val probabilities =
			Array.tabulate[Double](width)(i => if (i < spread) (spread - i) / normalizer else 0.0d)
		new Distribution(probabilities)
	}

	def zeta(exponent: Double, shift: Double, width: Int): Distribution = {
		assert(width >= 1)
		assert(exponent >= 0)
		assert(shift > 0)
		val values = Array.tabulate[Double](width)(i => Math.pow(i + shift, -exponent))
		val normalizer = values.sum
		val probabilities = values.map(_ / normalizer)
		new Distribution(probabilities)
	}

	def twoStep(spread: Double, width: Int): Distribution = {
		assert(width >= 1)
		assert(spread >= 0 && spread <= width)
		val height: Double = 1.0d / spread
		val remainder = (spread - spread.floor) / spread
		val probabilities = Array.tabulate[Double](width)(i =>
			if (i < spread.floor) {
				height
			} else if (i == spread.floor) {
				remainder
			} else {
				0.0d
			})
		new Distribution(probabilities)
	}
}
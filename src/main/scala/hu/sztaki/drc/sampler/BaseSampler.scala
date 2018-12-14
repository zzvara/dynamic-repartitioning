package hu.sztaki.drc.sampler

import hu.sztaki.drc.Naive
import hu.sztaki.drc.utilities.Configuration
import it.unimi.dsi.util.XoRoShiRo128PlusRandom

import scala.collection.mutable

//class Histogram {
//	var map: mutable.Map[Any, Double] = mutable.HashMap.empty[Any, Double]
//	var internalLoad: Double = 0.0d
//	var scale: Double = 1.0d
//
//	def width: Int = map.keySet.size
//
//	def isEmpty: Boolean = map.isEmpty
//
//	def value: Map[Any, Double] = map.toMap
//
//	def scaleDown(scalingFactor: Double): Unit = {
//		map.transform({ case (k, v) => v / scalingFactor })
//		internalLoad /= scalingFactor
//	}
//
//	def cutBack(boundary: Int): Unit =
//		map = mutable.HashMap.empty[Any, Double] ++ map.toSeq.sortBy(-_._2).take(boundary)
//
//	protected def add(k: Any, v: Double): Unit = {
//			val value = v * scale
//			map.put(k, map.getOrElse(k, 0.0d) + value)
//			internalLoad += value
//	}
//}

class BaseSampler {
	var version: Int = 0
	var map: mutable.Map[Any, Double] = mutable.HashMap.empty[Any, Double]
	var internalLoad: Double = 0.0d
	var externalLoad: Double = 0.0d
	var scale: Double = 1.0d
	var samplingRate: Double = 1.0d
	var keyComplexity: Option[KeyComplexity] = None
	val random = new XoRoShiRo128PlusRandom()

	def incrementVersion(): Unit = version += 1

	def setKeyComplexity(keyComplexity: KeyComplexity): Unit = this.keyComplexity = Some(keyComplexity)

	def width: Int = map.keySet.size

	def isEmpty: Boolean = map.isEmpty

	def isNormed: Boolean = Math.abs(internalLoad - 1.0d) < 0.001

	def value: Map[Any, Double] = map.toMap

//	def reset(map: mutable.Map[Any, Double],
//		version: Int,
//		internalLoad: Double,
//		externalLoad: Double,
//		scale: Double,
//		keyComplexity: Option[KeyComplexity] = None): Unit = {
//		// require(innerLoad >= map.values.sum)
//		this.map = map
//		this.version = version
//		this.internalLoad = internalLoad
//		this.externalLoad = externalLoad
//		this.scale = scale
//		this.keyComplexity = keyComplexity
//	}
//
//	def reset(): Unit =
//		reset(mutable.HashMap.empty[Any, Double], 0, 0.0d, 0.0d, 1.0d)
//
//	// assuming that `hist` is loss-free
//	def setHistogram(hist: Map[Any, Double]): Unit =
//		reset(mutable.HashMap.empty[Any, Double] ++ hist, version, hist.values.sum, hist.values.sum, 1.0d, keyComplexity)

	def scaleDown(scalingFactor: Double): Unit = {
		map.transform({ case (k, v) => v / scalingFactor })
		internalLoad /= scalingFactor
	}

	protected def getComplexity(k: Any): Double = keyComplexity match {
		case Some(kk) => kk.getComplexity(k)
		case None => 1.0d
	}

	def samplingCondition(k: Any): Boolean =
		random.nextDouble() <= samplingRate

	def performBackoff(k: Any): Unit = {}

	def cutBack(boundary: Int): Unit =
		map = mutable.HashMap.empty[Any, Double] ++ map.toSeq.sortBy(-_._2).take(boundary)

	def add(k: Any): Unit = {
		val value = getComplexity(k)
		externalLoad += value
		if (samplingCondition(k)) {
			map.put(k, map.getOrElse(k, 0.0d) + value * scale)
			internalLoad += value * scale
			performBackoff(k)
		}
	}
}

class NaiveSampler extends BaseSampler {
	protected val HISTOGRAM_SCALE_BOUNDARY: Int =
		Configuration.internal().getInt("repartitioning.data-characteristics.histogram-scale-boundary")
	protected val BACKOFF_FACTOR: Double =
		Configuration.internal().getDouble("repartitioning.data-characteristics.backoff-factor")
	protected val HISTOGRAM_SIZE_BOUNDARY: Int =
		Configuration.internal().getInt("repartitioning.data-characteristics.histogram-size-boundary")
	protected val HISTOGRAM_COMPACTION: Int =
		Configuration.internal().getInt("repartitioning.data-characteristics.histogram-compaction")

	var boundary: Int = HISTOGRAM_SCALE_BOUNDARY
//	var widthHistory: List[Int] = List.empty[Int]

	override def performBackoff(v: Any): Unit = {
		if (width >= boundary) {
			samplingRate = samplingRate / BACKOFF_FACTOR
			boundary += HISTOGRAM_SCALE_BOUNDARY
			scaleDown(BACKOFF_FACTOR)
			// Decide if additional cut is needed.
			if (width > HISTOGRAM_SIZE_BOUNDARY) {
//				widthHistory = widthHistory :+ width
				cutBack(HISTOGRAM_COMPACTION)
				boundary = HISTOGRAM_COMPACTION + HISTOGRAM_SCALE_BOUNDARY
			}
		}
	}
}

class ConceptierSampler extends NaiveSampler {
	protected val TAKE: Int =
		Configuration.internal().getInt("repartitioning.data-characteristics.take")
//	protected val HISTOGRAM_SCALE_BOUNDARY: Int =
//		Configuration.internal().getInt("repartitioning.data-characteristics.histogram-scale-boundary")
//	protected val BACKOFF_FACTOR: Double =
//		Configuration.internal().getDouble("repartitioning.data-characteristics.backoff-factor")
//	protected val HISTOGRAM_HARD_BOUNDARY: Int = HISTOGRAM_SOFT_BOUNDARY
//	protected val INITIAL_HARD_BOUNDARY: Int = HISTOGRAM_SOFT_BOUNDARY
//	protected val HISTOGRAM_COMPACTION: Int =
//		Configuration.internal().getInt("repartitioning.data-characteristics.histogram-compaction")
	protected val DRIFT_BOUNDARY: Double =
		Configuration.internal().getInt("repartitioning.data-characteristics.drift-boundary")
	protected val CONCEPT_SOLIDARITY: Int =
		Configuration.internal().getInt("repartitioning.data-characteristics.concept-solidarity")
	protected val DRIFT_HISTORY_WEIGHT: Double =
		Configuration.internal().getInt("repartitioning.data-characteristics.drift-history-weight")

	var consecutiveConceptSolidarity: Int = 0
	var driftHistory: Double = DRIFT_BOUNDARY
//	var drifts: List[Double] = List.empty[Double]
	var keySetHistory: Set[Any] = Set.empty[Any]
//	var widthHistory: List[Int] = List.empty[Int]
//	var boundary: Int = HISTOGRAM_SCALE_BOUNDARY

	override def performBackoff(v: Any): Unit = {
		if (width >= boundary) {
//			widthHistory = widthHistory :+ width
			samplingRate = samplingRate / BACKOFF_FACTOR
			scale = scale * BACKOFF_FACTOR
			val sortedMap = map.toSeq.sortBy(-_._2)
			val currentTop = sortedMap.take(TAKE)
			val currentKeySet = currentTop.map(_._1).toSet

			if (keySetHistory != null) {
				val missingKeys = keySetHistory -- currentKeySet
				val newPositionsOfFallingKeys = mutable.Map[Any, (Int, Double)]() // Key -> (Position, Frequency)
				missingKeys.map {
					key =>
						val position = sortedMap.indexWhere(_._1 == key)
						newPositionsOfFallingKeys.put(key,
							(if (position == -1) { width } else { position }, map.getOrElse(key, 0.0))
						)
				}
				val minimumItemInTop = currentTop.last
				val maximumDistance = (((minimumItemInTop._2 - TAKE + width) +
					(minimumItemInTop._2 - (2 * TAKE) + width)) / 2) * TAKE
				val currentDistance = newPositionsOfFallingKeys.map {
					case (k, (p, f)) =>
						require(p > TAKE - 1)
						(p - TAKE) + (minimumItemInTop._2 - f)
				}.sum
				val drift = currentDistance / maximumDistance
				driftHistory = (driftHistory * DRIFT_HISTORY_WEIGHT) + (drift * (1 - DRIFT_HISTORY_WEIGHT))
//				drifts = drifts :+ drift

				if (driftHistory > DRIFT_BOUNDARY) {
					boundary += HISTOGRAM_SCALE_BOUNDARY
					consecutiveConceptSolidarity = 0
				} else {
					consecutiveConceptSolidarity += 1
					// Condition doesn't look right???
					if (consecutiveConceptSolidarity >= CONCEPT_SOLIDARITY &&
						boundary > HISTOGRAM_SCALE_BOUNDARY) {
						boundary += HISTOGRAM_SCALE_BOUNDARY
						consecutiveConceptSolidarity = 0
					}
					cutBack(boundary - HISTOGRAM_COMPACTION)
				}
			} else {
				boundary += HISTOGRAM_SCALE_BOUNDARY
				consecutiveConceptSolidarity = 0
			}
			keySetHistory = currentKeySet
		}
	}
}

class KeyComplexity(complexityMap: Map[Any, Double]) {

	def this() = {
		this(Map.empty[Any, Double])
	}

	def getComplexity(key: Any): Double = {
		complexityMap.get(key) match {
			case Some(c) => c
			case None => KeyComplexity.DEFAULT_COMPLEXITY
		}
	}
}

object KeyComplexity {
	// default complexity for keys not in complexityMap
	private val DEFAULT_COMPLEXITY: Double = 1.0d
}
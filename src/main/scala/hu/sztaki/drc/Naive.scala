package hu.sztaki.drc

import hu.sztaki.drc.utilities.{Configuration, Logger}
import it.unimi.dsi.util.XoRoShiRo128PlusRandom
import Conceptier._

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

object Conceptier {
  type Key = Any
  type Position = Int
  type Frequency = Double
}

trait Conceptier extends Sampling {
  protected val TAKE: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.take")
  protected val HISTOGRAM_SOFT_BOUNDARY: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-scale-boundary")
  protected val BACKOFF_FACTOR: Double =
    Configuration.internal().getDouble("repartitioning.data-characteristics.backoff-factor")
  protected var HISTOGRAM_HARD_BOUNDARY: Int
  protected val INITIAL_HARD_BOUNDARY: Int = HISTOGRAM_HARD_BOUNDARY
  protected val HISTOGRAM_COMPACTION: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-compaction")
  protected val DRIFT_BOUNDARY: Double =
    Configuration.internal().getInt("repartitioning.data-characteristics.drift-boundary")
  protected val CONCEPT_SOLIDARITY: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.concept-solidarity")
  protected val DRIFT_HISTORY_WEIGHT: Double =
    Configuration.internal().getInt("repartitioning.data-characteristics.drift-history-weight")

  protected var consecutiveConceptSolidarity = 0

  var driftHistory: Double = DRIFT_BOUNDARY

  protected var drifts = List.empty[Double]

  var history: Set[Any] = _

  var _sampleScale = 1.0

  def driftList = drifts

  protected var histogramCompaction = HISTOGRAM_COMPACTION

  def add(v: (Any, Double)): Unit = {
    _recordsPassed += 1
    if (random.nextDouble() <= _sampleRate) {
      (v._1, {
        map.get(v._1) match {
          case Some(value) => map.put(v._1, value + _sampleScale)
          case None =>
            _width += 1
            map.put(v._1, _sampleScale)
        }


        /**
          * A histogram size boundary has been reached. Maybe cut, maybe not. Let's see.
          */
        if (_width >= HISTOGRAM_HARD_BOUNDARY) {
          _widthHistory = _widthHistory :+ _width
          _sampleRate = _sampleRate / BACKOFF_FACTOR
          _sampleScale = _sampleScale * BACKOFF_FACTOR
          _backoffsPerformed += 1
          /**
            * Sort the histogram.
            */
          val sortedMap = map.toSeq.sortBy(-_._2)

          val currentTop = sortedMap.take(TAKE)
          val currentKeySet = currentTop.map(_._1).toSet

          /**
            * Load the last top-k (history) and retrieve the current as well.
            */
          if (history != null) {
            /**
              * @note Calculate this drift, this is the brainer.
              */
            val missingKeys = history -- currentKeySet
            // println(s"Total [$missingKeys] are missing from the new top-k.")
            // Search for the missing items in the current, whole histogram.
            val newPositionsOfFallingKeys = mutable.Map[Key, (Position, Frequency)]()
            missingKeys.map {
              key =>
                newPositionsOfFallingKeys.put(key,
                  (sortedMap.indexWhere(_._1 == key), map(key))
                )
            }
            val minimumItemInTop = currentTop.last
            val maximumDistance = (((minimumItemInTop._2 - TAKE + _width) +
              (minimumItemInTop._2 - (2 * TAKE) + _width)) / 2) * TAKE
            val currentDistance = newPositionsOfFallingKeys.map {
              case (k, (p, f)) =>
                require(p > TAKE - 1)
                (p - TAKE) + (minimumItemInTop._2 - f)
            }.sum

            // println(s"Current distance is [$currentDistance] and maximum distance is [$maximumDistance].")

            val drift = currentDistance / maximumDistance
            driftHistory = (driftHistory * DRIFT_HISTORY_WEIGHT) + (drift * (1 - DRIFT_HISTORY_WEIGHT))
            drifts = drifts :+ drift

            // println(s"Current drift is [$drift] and drift history is [$driftHistory].")

            if (driftHistory > DRIFT_BOUNDARY) {
              /**
                * Too much drift, increase the boundary, just to be safe.
                * Do not cut.
                */
              HISTOGRAM_HARD_BOUNDARY = HISTOGRAM_HARD_BOUNDARY + HISTOGRAM_SOFT_BOUNDARY
              consecutiveConceptSolidarity = 0
            } else {
              /**
                * Not too much drift.
                */
              consecutiveConceptSolidarity += 1
              /**
                * If concept drift not detected for a long time, and the hard-boundary can be
                * moved one step to the left.
                */
              if (consecutiveConceptSolidarity >= CONCEPT_SOLIDARITY &&
                HISTOGRAM_HARD_BOUNDARY > INITIAL_HARD_BOUNDARY) {
                /**
                  * Step back with one boundary.
                  */
                HISTOGRAM_HARD_BOUNDARY = HISTOGRAM_HARD_BOUNDARY - HISTOGRAM_SOFT_BOUNDARY
                consecutiveConceptSolidarity = 0
              }
              /**
                * Cut with the current boundary - maybe changed in the last step.
                */
              // println("Cutting.")
              val temporaryMap = HashMap.empty[Any, Double]
              sortedMap.take(HISTOGRAM_HARD_BOUNDARY - HISTOGRAM_COMPACTION).foreach {
                pair => temporaryMap.put(pair._1, pair._2)
              }
              map = temporaryMap
              _width = HISTOGRAM_HARD_BOUNDARY - HISTOGRAM_COMPACTION
            }
          } else {
            HISTOGRAM_HARD_BOUNDARY = HISTOGRAM_HARD_BOUNDARY + HISTOGRAM_SOFT_BOUNDARY
            consecutiveConceptSolidarity = 0
          }

          /**
            * Update the history.
            */
          history = currentKeySet
        }
      })

    }
  }

  def mergeWith(other: Sampling): Unit = other match {
    case o: Conceptier =>
      _width = o._width
      _recordsPassed = o._recordsPassed
      _sampleRate = o._sampleRate
      _version = o._version
      map = o.map
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge [${this.getClass.getName}] with [${other.getClass.getName}]!")
  }
}

trait Naive extends Sampling {
  protected val TAKE: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.take")
  protected val HISTOGRAM_SCALE_BOUNDARY: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-scale-boundary")
  protected val BACKOFF_FACTOR: Double =
    Configuration.internal().getDouble("repartitioning.data-characteristics.backoff-factor")
  protected val HISTOGRAM_SIZE_BOUNDARY: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-size-boundary")
  protected val HISTOGRAM_COMPACTION: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-compaction")

  private var _nextScaleBoundary: Int = HISTOGRAM_SCALE_BOUNDARY
  private var histogramCompaction = HISTOGRAM_COMPACTION

  def add(v: (Any, Double)): Unit = {
    _recordsPassed += 1
    if (random.nextDouble() <= _sampleRate) {
      (v._1, {
        map.get(v._1) match {
          case Some(value) => map.put(v._1, value + 1)
          case None =>
            _width += 1
            map.put(v._1, 1)
        }
      })

      if (_width >= _nextScaleBoundary) {
        _sampleRate = _sampleRate / BACKOFF_FACTOR
        _nextScaleBoundary += HISTOGRAM_SCALE_BOUNDARY
        _backoffsPerformed += 1
        map.transform {
          case (_, x) => x / BACKOFF_FACTOR
        }

        // Decide if additional cut is needed.
        if (_width > HISTOGRAM_SIZE_BOUNDARY) {
          _widthHistory = _widthHistory :+ _width
          _width = histogramCompaction
          _nextScaleBoundary = _width + HISTOGRAM_SCALE_BOUNDARY
          val temporaryMap = HashMap.empty[Any, Double]
          map.toSeq.sortBy(-_._2).take(histogramCompaction).foreach {
            pair => temporaryMap.put(pair._1, pair._2)
          }
          map = temporaryMap
        }
      }
    }
  }

  def mergeWith(other: Sampling): Unit = other match {
    case o: Naive =>
      _width = o._width
      _recordsPassed = o._recordsPassed
      _sampleRate = o._sampleRate
      _version = o._version
      map = o.map
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge [${this.getClass.getName}] with [${other.getClass.getName}]!")
  }


}

trait Sampling extends Logger {
  val random = new XoRoShiRo128PlusRandom()

  protected var map: mutable.Map[Any, Double] = mutable.HashMap.empty[Any, Double]

  protected var _widthHistory: List[Int] = List.empty
  def widthHistory: List[Int] = _widthHistory

  protected var _sampleRate: Double = 1.0
  def sampleRate: Double = _sampleRate

  protected var _backoffsPerformed: Int = 0
  def backoffsPerformed: Int = _backoffsPerformed

  protected var _width: Int = 0
  def width: Int = _width

  protected var _recordsPassed: Long = 0
  def recordsPassed: Long = _recordsPassed

  protected var _version: Int = 0
  def version: Int = _version
  def incrementVersion(): Unit = { _version += 1 }

  def isEmpty: Boolean = map.isEmpty
  def value: Map[Any, Double] = map.toMap
  def reset(): Unit = map = map.empty
  def setValue(values: Map[Any, Double]): Unit = {
    values.foreach(value => map.put(value._1, value._2))
  }

  def normalize(histogram: Map[Any, Double], normalizationParam: Long): Map[Any, Double] = {
    val normalizationFactor = normalizationParam * sampleRate
    histogram.mapValues(_ / normalizationFactor)
  }

  def add(v: (Any, Double)): Unit
}

object Naive {
  def merge[A, B](zero: B)(f: (B, B) => B)(s1: Map[A, B], s2: Map[A, B]): Map[A, B] = {
    s1 ++ s2.map{ case (k, v) => k -> f(v, s1.getOrElse(k, zero)) }
  }

  def weightedMerge[A](zero: Double, weightOfFirst: Double)
                      (s1: Map[A, Double], s2: Seq[(A, Double)]): Seq[(A, Double)] = {
    val weightedS1 = s1.map(pair => (pair._1, pair._2 * weightOfFirst))
    (
      weightedS1 ++
        s2.map{ case (k, v) => k -> (v * (1 - weightOfFirst) + weightedS1.getOrElse(k, zero)) }
      ).toSeq
  }

  def isWeightable[T]()(implicit mf: ClassTag[T]): Boolean = ???

  def className[T]()(implicit mf: ClassTag[T]): String =
    mf.runtimeClass.getCanonicalName
}


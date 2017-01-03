package hu.sztaki.drc

import hu.sztaki.drc.utilities.{Configuration, Logger}

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

trait Sampler extends Logger {
  protected val TAKE: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.take")
  protected val HISTOGRAM_SCALE_BOUNDARY: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-scale-boundary")
  protected val BACKOFF_FACTOR: Double =
    Configuration.internal().getDouble("repartitioning.data-characteristics.backoff-factor")
  protected val DROP_BOUNDARY: Double =
    Configuration.internal().getDouble("repartitioning.data-characteristics.drop-boundary")
  protected val HISTOGRAM_SIZE_BOUNDARY: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-size-boundary")
  protected val HISTOGRAM_COMPACTION: Int =
    Configuration.internal().getInt("repartitioning.data-characteristics.histogram-compaction")

  private var _map: Map[Any, Double] = HashMap[Any, Double]()

  /**
    * Rate in which records are put into the histogram.
    * Value represent that each n-th input is recorded.
    */
  private var _sampleRate: Double = 1.0
  def sampleRate: Double = _sampleRate
  /**
    * Size or width of the histogram, that is equal to the size of the map.
    */
  private var _width: Int = 0
  def width: Int = _width

  private var _recordsPassed: Long = 0
  def recordsPassed: Long = _recordsPassed

  private var _nextScaleBoundary: Int = HISTOGRAM_SCALE_BOUNDARY

  private var _version: Int = 0
  def version: Int = _version
  def incrementVersion(): Unit = { _version += 1 }

  def updateTotalSlots(totalSlots: Int): Unit = {
    histogramCompaction = Math.max(histogramCompaction, totalSlots * 2)
    logInfo(s"Updated histogram compaction level based on $totalSlots number" +
            s" of total slots, to $histogramCompaction.")
  }

  private var histogramCompaction = HISTOGRAM_COMPACTION

  def normalize(histogram: Map[Any, Double], normalizationParam: Long): Map[Any, Double] = {
    val normalizationFactor = normalizationParam * sampleRate
    histogram.mapValues(_ / normalizationFactor)
  }

  def increase(pair: Product2[Any, Any]): Double = 1.0

  def reset(): Unit = _map = _map.empty

  def add(v: (Any, Double)): Unit = {
    val pair = v
    _recordsPassed += 1
    if (Math.random() <= _sampleRate) { // Decided to record the key.
    val updatedHistogram = _map + ((pair._1, {
      val newValue = _map.get(pair._1) match {
        case Some(value) => value + increase(pair)
        case None =>
          _width = _width + 1
          increase(pair)
      }
      newValue
    }
    ))
      // Decide if scaling is needed.
      if (_width >= _nextScaleBoundary) {
        _sampleRate = _sampleRate / BACKOFF_FACTOR
        _nextScaleBoundary += HISTOGRAM_SCALE_BOUNDARY
        val scaledHistogram =
          updatedHistogram
            .mapValues(x => x / BACKOFF_FACTOR)
            .filter(p => p._2 > DROP_BOUNDARY)

        // Decide if additional cut is needed.
        if (_width > HISTOGRAM_SIZE_BOUNDARY) {
          _width = histogramCompaction
          _nextScaleBoundary = _width + HISTOGRAM_SCALE_BOUNDARY
          _map = scaledHistogram.toSeq.sortBy(-_._2).take(histogramCompaction).toMap
        } else {
          _map = scaledHistogram
        }
      } else { // No need to cut the histogram.
        _map = updatedHistogram
      }
    } // Else histogram does not change.
  }

  def merge(other: Sampler): Unit = other match {
    case o: Sampler =>
      _width = o._width
      _recordsPassed = o._recordsPassed
      _sampleRate = o._sampleRate
      _version = o._version
      _map = o._map
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge [${this.getClass.getName}] with [${other.getClass.getName}]!")
  }

  def isEmpty: Boolean = _map.isEmpty

  /**
    * Defines the current value of this accumulator.
    */
  def value: Map[Any, Double] = _map

  def setValue(values: Map[Any, Double]): Unit = {
    _map = values
  }
}


object Sampler {
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


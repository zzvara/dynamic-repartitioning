package hu.sztaki.drc

abstract class ScannerFactory[+S <: Scanner[_, _]] extends Serializable {
  def apply(totalSlots: Int,
            histogramDrop: (Int, Long, Int, Naive) => Unit): S
}

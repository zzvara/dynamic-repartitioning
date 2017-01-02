package hu.sztaki.drc.partitioner

object BinarySearch {
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
}
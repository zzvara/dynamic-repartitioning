package hu.sztaki.drc

import hu.sztaki.drc.partitioner.{KeyIsolatorPartitioner, PartitioningInfo}

// old test
// maybe rewrite for new partitioner
object KeyIsolationPartitionerTest {

//  def main(args: Array[String]): Unit = {
//    test1()
//
//    test2()
//
//    test3()
//
//    test4()
//  }
//
//  def test1() = {
//    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55, 6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)
//
//    val p = 6
//    val pCut = 4
//    val level = 9.0d / 55
//    val sCut = 2
//    //    val block = 18.0d / 55
//
//    val weights = Array[Double](2.0d / 55, 1.0d / 55)
//    val info = new PartitioningInfo(p, pCut, sCut, level, Array[Any](6, 5, 1, 9), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55))
//
//    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
//      case dd: Int => (dd % 21).toDouble / 21
//      case _ => 0.0d
//    })
//
//    val partitioner = new KeyIsolatorPartitioner(info, weightedPartitioner)
//
//    assert(partitioner.size == 6)
//
//    assert(partitioner.get(6) == 0)
//    assert(partitioner.get(5) == 1)
//    assert(partitioner.get(1) == 2)
//    assert(partitioner.get(9) == 3)
//    assert(partitioner.get(21) == 5)
//    assert(partitioner.get(29) == 5)
//    assert(partitioner.get(38) == 4)
//    assert(partitioner.get(39) == 4)
//    assert(partitioner.get(40) == 3)
//    assert(partitioner.get(41) == 2) //rounding error, should be 3
//  }
//
//  def test2() = {
//    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
//      6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)
//
//    val p = 7
//    val pCut = 3
//    val level = 7.0d / 55
//    val sCut = 3
//    // val block = 28.0d / 55
//
//    val weights = Array[Double]()
//    val info = new PartitioningInfo(p, pCut, sCut, level,  Array[Any](6, 5, 1), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55))
//
//    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
//      case dd: Int => (dd % 28).toDouble / 28
//      case _ => 0.0d
//    })
//
//    val partitioner = new KeyIsolatorPartitioner(info, weightedPartitioner)
//
//    assert(partitioner.size == 7)
//
//    assert(partitioner.get(6) == 0)
//    assert(partitioner.get(5) == 1)
//    assert(partitioner.get(1) == 2)
//    assert(partitioner.get(28) == 6)
//    assert(partitioner.get(34) == 6)
//    assert(partitioner.get(41) == 5)
//    assert(partitioner.get(42) == 5)
//    assert(partitioner.get(48) == 4)
//    assert(partitioner.get(55) == 3)
//  }
//
//  def test3() = {
//    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
//      6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)
//
//    val p = 5
//    val pCut = 3
//    val level = 1.0d / 5
//    val sCut = 0
//    // val block = 2.0d / 5
//
//    val weights = Array[Double](3.0d / 55, 2.0d / 55, 1.0d / 55)
//    val info = new PartitioningInfo(p, pCut, sCut, level, Array[Any](4, 3, 1), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55))
//
//    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
//      case dd: Int => (dd % 28).toDouble / 28
//      case _ => 0.0d
//    })
//
//    val partitioner = new KeyIsolatorPartitioner(info, weightedPartitioner)
//
//    assert(partitioner.size == 5)
//
//    assert(partitioner.get(4) == 0)
//    assert(partitioner.get(3) == 1)
//    assert(partitioner.get(1) == 2)
//    assert(partitioner.get(28) == 4)
//    assert(partitioner.get(38) == 4)
//    assert(partitioner.get(48) == 3)
//    assert(partitioner.get(50) == 3)
//    assert(partitioner.get(52) == 2)
//    assert(partitioner.get(54) == 1)
//    assert(partitioner.get(55) == 0) //rounding error, should be 1
//  }
//
//  def test4() = {
//    val distribution = Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
//      6.0d / 55, 5.0d / 55, 4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)
//
//    val p = 6
//    val pCut = 6
//    val level = 9.0d / 55
//    val sCut = 2
//    // val block = 0.0d
//
//    val weights = Array[Double](4.0d / 55, 3.0d / 55, 2.0d / 55, 1.0d / 55)
//    val info = new PartitioningInfo(p, pCut, sCut, level, Array[Any](0, 1, 2, 3, 4, 5), Array[Double](10.0d / 55, 9.0d / 55, 8.0d / 55, 7.0d / 55,
//      6.0d / 55, 5.0d / 55))
//
//    val weightedPartitioner = new WeightedHashPartitioner(weights, info, {
//      case dd: Int => (dd % 10).toDouble / 10
//      case _ => 0.0d
//    })
//
//    val partitioner = new KeyIsolatorPartitioner(info, weightedPartitioner)
//
//    assert(partitioner.size == 6)
//
//    assert(partitioner.get(0) == 0)
//    assert(partitioner.get(1) == 1)
//    assert(partitioner.get(2) == 2)
//    assert(partitioner.get(3) == 3)
//    assert(partitioner.get(4) == 4)
//    assert(partitioner.get(5) == 5)
//    assert(partitioner.get(10) == 5)
//    assert(partitioner.get(13) == 5)
//    assert(partitioner.get(16) == 4)
//    assert(partitioner.get(17) == 4)
//    assert(partitioner.get(18) == 3)
//  }

}
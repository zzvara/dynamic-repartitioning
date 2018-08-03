package hu.sztaki.drc

object LossyTest {
  def main(args: Array[String]): Unit = {
    val lossy = new Lossy(0.1, 0.001)

    1 to 10000 foreach {
      lossy.addAny(_)
    }

    assert(lossy.value.size == 10000)
  }
}

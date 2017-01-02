package hu.sztaki.drc

abstract class StrategyFactory[+S <: Strategy] extends Serializable {
  def apply(stageID: Int, attemptID: Int, numPartitions: Int,
            resourceStateHandler: Option[() => Int] = None): S
}

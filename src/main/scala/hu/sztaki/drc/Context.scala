package hu.sztaki.drc

abstract class Context[M <: Metrics[M]] {
  /**
    * In Flink, this is the operator ID.
    */
  def stageID(): Int
  /**
    * Attempt ID.
    */
  def attemptID(): Int
  /**
    * In Flink, this is the sub-task ID.
    */
  def partitionID(): Int
  def attemptNumber(): Int
  def metrics(): M
}

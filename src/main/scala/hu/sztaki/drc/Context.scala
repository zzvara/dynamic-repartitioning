package hu.sztaki.drc

abstract class Context[M <: Metrics[M]] {
  def stageID(): Int
  def attemptID(): Int
  def partitionID(): Int
  def attemptNumber(): Int
  def metrics(): M
}

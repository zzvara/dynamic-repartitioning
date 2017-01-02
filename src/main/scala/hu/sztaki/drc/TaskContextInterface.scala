package hu.sztaki.drc

abstract class TaskContextInterface[TaskMetrics <: TaskMetricsInterface[TaskMetrics]] {
  def taskMetrics(): TaskMetrics
  def partitionID(): Int
  def attemptId(): Int
  def stageId(): Int
  def attemptNumber(): Int
}

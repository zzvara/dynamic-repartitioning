package hu.sztaki.drc.messages

import hu.sztaki.drc.{Scanner, ScannerFactory, TaskContextInterface, TaskMetricsInterface}

/**
  * Scan strategy message sent to workers.
  */
case class StandaloneStrategy[TaskContext <: TaskContextInterface[TaskMetrics],
                              TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  stageID: Int,
  scanner: ScannerFactory[Scanner[TaskContext, TaskMetrics]])
extends ScanStrategy

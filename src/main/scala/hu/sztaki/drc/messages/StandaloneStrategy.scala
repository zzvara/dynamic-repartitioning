package hu.sztaki.drc.messages

import hu.sztaki.drc.{Scanner, ScannerFactory, Context, Metrics}

/**
  * Scan strategy message sent to workers.
  */
case class StandaloneStrategy[TaskContext <: Context[TaskMetrics],
                              TaskMetrics <: Metrics[TaskMetrics]](
  stageID: Int,
  scanner: ScannerFactory[Scanner[TaskContext, TaskMetrics]])
extends ScanStrategy

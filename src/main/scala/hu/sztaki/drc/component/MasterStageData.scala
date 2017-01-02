package hu.sztaki.drc.component

import hu.sztaki.drc.messages.StandaloneStrategy
import hu.sztaki.drc.{RepartitioningModes, Strategy, TaskContextInterface, TaskMetricsInterface}

case class MasterStageData[TaskContext <: TaskContextInterface[TaskMetrics],
                           TaskMetrics <: TaskMetricsInterface[TaskMetrics]](
  stageID: Int,
  strategy: Strategy,
  mode: RepartitioningModes.Value,
  scanStrategy: StandaloneStrategy[TaskContext, TaskMetrics])

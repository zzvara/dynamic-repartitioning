package hu.sztaki.drc.component

import hu.sztaki.drc.messages.{ScanStrategies, StandaloneStrategy}
import hu.sztaki.drc._
import hu.sztaki.drc.utilities.Logger

import scala.collection.mutable

trait StreamingRepartitioningTrackerWorkerHelper[
  TaskContext <: TaskContextInterface[TaskMetrics],
  TaskMetrics <: TaskMetricsInterface[TaskMetrics]] extends Logger {
  protected val streamData = mutable.HashMap[Int, RepartitioningStreamData]()

  protected def getStageData: mutable.HashMap[Int,
    RepartitioningStageData[TaskContext, TaskMetrics]]

  protected def privateReceive: PartialFunction[Any, Unit] = {
    case ScanStrategies(scanStrategies) =>
      logInfo(s"Received a list of scan strategies, with size of ${scanStrategies.length}.")
      scanStrategies.foreach {
        /**
          * @todo The standalone part should only be in the default tracker.
          */
        case StandaloneStrategy(stageID, scanner) =>
          getStageData.update(
            stageID,
            RepartitioningStageData[TaskContext, TaskMetrics](
              scanner.asInstanceOf[ScannerFactory[Scanner[TaskContext, TaskMetrics]]]))
        case StreamingScanStrategy(streamID, strategy, parentStreams) =>
          logInfo(s"Received streaming strategy for stream ID $streamID.")
          streamData.update(streamID, RepartitioningStreamData(streamID, strategy, parentStreams))
      }
    case StreamingScanStrategy(streamID, strategy, parentStreams) =>
      logInfo(s"Received streaming strategy for stream ID $streamID.")
      streamData.update(streamID, RepartitioningStreamData(streamID, strategy, parentStreams))
  }
}

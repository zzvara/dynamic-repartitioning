package hu.sztaki.drc.component

import hu.sztaki.drc.utilities.{Configuration, Logger}
import hu.sztaki.drc.{Sampling, StreamingDecider}

import scala.collection.mutable

trait StreamingRepartitioningTrackerMasterHelper[Stream <: { def ID: Int; def numPartitions: Int }] extends Logger {
  protected val _streamData = mutable.HashMap[Int, StreamState]()

  protected var isInitialized = false

  /**
    * Please not that here we are importing a companion object, marked with sign `$`.
    */
  protected val deciderFactoryClass =
    Class.forName(
      Configuration.internal().getString("repartitioning.streaming.decider.factory") + "$",
      true,
      Thread.currentThread().getContextClassLoader
    ).asInstanceOf[Class[hu.sztaki.drc.utilities.Factory.forStreamingDecider[Stream]]]

  logInfo(s"The decider factory class is [${deciderFactoryClass.getClass.getName}].")

  protected val deciderFactory = deciderFactoryClass
      .getField("MODULE$")
      .get(deciderFactoryClass)
      .asInstanceOf[hu.sztaki.drc.utilities.Factory.forStreamingDecider[Stream]]

  def getTotalSlots: Int

  protected def getNumberOfPartitions(streamID: Int): Int

  def updateLocalHistogramForStreaming(
    stream: Stream,
    taskID: Long,
    partitionID: Int,
    dataCharacteristics: Sampling
  ): Unit = {
    _streamData.find { _._2.hasParent(stream.ID) } match {
      case Some((sID, streamData)) =>
        logInfo(s"Updating local histogram for task $taskID " +
                s"in stream ${stream.ID} with output stream ID $sID.")
        streamData.strategies.getOrElseUpdate(
          stream.ID,
          deciderFactory(stream.ID, stream, 1, Some(() => getTotalSlots))
        ).onHistogramArrival(partitionID, dataCharacteristics)
      case None => logWarning(
          s"Could not update local histogram for streaming," +
          s" since streaming data does not exist for DStream" +
          s" ID ${stream.ID}!")
    }
  }

  protected def updatePartitionMetrics(
    stream: Stream,
    taskID: Long,
    partitionID: Int,
    recordsRead: Long
  ): Unit = {
    _streamData.find {
      _._2.hasParent(stream.ID)
    } match {
      case Some((sID, streamData)) =>
        logInfo(s"Updating partition metrics for task $taskID " +
                s"in stream ${stream.ID}.")
        val id = stream.ID
        streamData.strategies.getOrElseUpdate(
          id,
          deciderFactory(stream.ID, stream, 1, Some(() => getTotalSlots))
        ).asInstanceOf[StreamingDecider[Stream]].onPartitionMetricsArrival(partitionID, recordsRead)
      case None => logWarning(
        s"Could not update local histogram for streaming," +
        s" since streaming data does not exist for DStream" +
        s" ID ${stream.ID}!")
    }
  }
}


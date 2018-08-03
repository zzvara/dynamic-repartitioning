package hu.sztaki.drc.component

import hu.sztaki.drc.StreamingDecider

import scala.collection.mutable
import scala.collection.mutable.Set

/**
  * @param stream output DStream
  * @param relatedJobs Mini-batches or jobs which has been spawned by this stream.
  * @param parentDStreams All the parent DStreams of the output DStream.
  */
case class StreamState[S <: SimpleStream](
  stream: S,
  relatedJobs: Set[Int] = Set[Int](),
  parentDStreams: scala.collection.immutable.Set[Int] = scala.collection.immutable.Set[Int](),
  scanStrategy: StreamingDecider[_ <: SimpleStream]) {

  def streamID: Int = stream.ID

  def numPartitions: Int = stream.numPartitions

    /**
      * Deciders, which are StreamingStrategies by default for each
      * inner stage. Stages are identified in a lazy manner when a task finished.
      * A task holds a corresponding DStream ID, which defines a reoccurring
      * stage in a mini-batch.
      */
    val strategies: mutable.Map[Int, StreamingDecider[_ <: SimpleStream]] =
      mutable.Map[Int, StreamingDecider[_ <: SimpleStream]]()

    def addJob(jobID: Int): StreamState[_ <: SimpleStream] = {
      relatedJobs += jobID
      this
    }

    def hasParent(stream: Int): Boolean = {
      parentDStreams.contains(stream)
    }
  }

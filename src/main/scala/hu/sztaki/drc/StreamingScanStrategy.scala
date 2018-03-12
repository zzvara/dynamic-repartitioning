package hu.sztaki.drc

import hu.sztaki.drc.messages.ScanStrategy

/**
  * Scan strategy message sent to workers.
  */
case class StreamingScanStrategy(
  streamID: Int,
  strategy: StreamingDecider[_ <: { def numPartitions: Int }],
  parentStreams: collection.immutable.Set[Int])
extends ScanStrategy

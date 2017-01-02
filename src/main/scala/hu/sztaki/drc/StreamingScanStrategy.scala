package hu.sztaki.drc

import hu.sztaki.drc.messages.ScanStrategy

/**
  * Scan strategy message sent to workers.
  */
private[drc] case class StreamingScanStrategy(
  streamID: Int,
  strategy: StreamingDecider[_],
  parentStreams: collection.immutable.Set[Int])
extends ScanStrategy

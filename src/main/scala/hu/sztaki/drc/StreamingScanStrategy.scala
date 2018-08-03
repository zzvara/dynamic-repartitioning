package hu.sztaki.drc

import hu.sztaki.drc.component.SimpleStream
import hu.sztaki.drc.messages.ScanStrategy

/**
  * Scan strategy message sent to workers.
  */
case class StreamingScanStrategy(
  streamID: Int,
  strategy: StreamingDecider[_ <: SimpleStream],
  parentStreams: collection.immutable.Set[Int])
extends ScanStrategy

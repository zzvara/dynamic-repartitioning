package hu.sztaki.drc.component

import hu.sztaki.drc.StreamingDecider

case class RepartitioningStreamState(
  streamID: Int,
  strategy: StreamingDecider[_],
  parentStreams: collection.immutable.Set[Int])

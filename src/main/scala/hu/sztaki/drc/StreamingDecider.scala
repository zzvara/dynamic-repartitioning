package hu.sztaki.drc

import hu.sztaki.drc.partitioner.KeyIsolatorPartitioner.Factory

abstract class StreamingDecider[Stream](
	streamID: Int,
	stream: Stream,
	nPartitions: Int,
	val perBatchSamplingRate: Int = 1,
	resourceStateHandler: Option[() => Int] = None)
extends Decider(streamID, nPartitions, resourceStateHandler) {
	def onPartitionMetricsArrival(partitionID: Int, recordsRead: Long): Unit
}

package hu.sztaki.drc.component

import hu.sztaki.drc.utilities.{Logger, Messageable}

/**
  * Common interface for each repartitioning tracker.
  */
abstract class RepartitioningTracker[MasterReference <: Messageable]
extends Logger {
  var master: MasterReference = _
}

object RepartitioningTracker {
  val MASTER_ENDPOINT_NAME = "RepartitioningTrackerMaster"
  val WORKER_ENDPOINT_NAME = "RepartitioningTrackerWorker"
}
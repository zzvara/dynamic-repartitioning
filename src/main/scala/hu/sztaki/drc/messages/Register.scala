package hu.sztaki.drc.messages

/**
  * Registering message sent from workers.
  */
case class Register[ComponentReference](executorID: String, workerReference: ComponentReference)
extends RepartitioningTrackerMessage

package hu.sztaki.drc

/**
 * Enumeration for repartitioning modes. These settings are global right now,
 * not stage based.
 */
object RepartitioningModes extends Enumeration {
  val ON, ONLY_ONCE, OFF = Value
}

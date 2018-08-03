package hu.sztaki.drc

/**
 * Enumeration for repartitioning modes. These settings are global right now,
 * not stage based.
 */
object Mode extends Enumeration {
  val Enabled, Once, Disabled = Value
}

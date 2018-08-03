package hu.sztaki.drc.partitioner

trait MigrationCostEstimator {

  def getMigrationCostEstimation: Option[Double]

}
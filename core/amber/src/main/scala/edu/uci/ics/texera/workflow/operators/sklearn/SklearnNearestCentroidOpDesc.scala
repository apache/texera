package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnNearestCentroidOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.neighbors import NearestCentroid"
  override def getUserFriendlyModelName = "Nearest Centroid"
}

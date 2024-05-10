package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnNearestCentroidOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.neighbors import NearestCentroid"
  modelUserFriendlyName = "Nearest Centroid"
}

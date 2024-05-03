package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnNCOp extends SklearnMLOp {
  modelImport = "from sklearn.neighbors import NearestCentroid"
  model = "NearestCentroid()"
  operatorName = "Nearest Centroid"
}

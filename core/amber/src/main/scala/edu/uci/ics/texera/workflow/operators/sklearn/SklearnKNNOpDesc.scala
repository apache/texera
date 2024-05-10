package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnKNNOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.neighbors import KNeighborsClassifier"
  modelUserFriendlyName = "K-nearest Neighbors"
}

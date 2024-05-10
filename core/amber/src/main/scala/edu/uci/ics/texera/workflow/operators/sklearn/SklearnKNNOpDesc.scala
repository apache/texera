package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnKNNOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.neighbors import KNeighborsClassifier"
  modelName = "K-nearest Neighbors"
}

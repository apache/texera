package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnBaggingOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.ensemble import BaggingClassifier"
  name = "Bagging"
}

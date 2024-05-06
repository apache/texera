package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnDummyClassifierOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.dummy import dummy"
  name = "Dummy Classifier"
}

package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnDummyClassifierOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.dummy import dummy"
  modelUserFriendlyName = "Dummy Classifier"
}

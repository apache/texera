package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnBaggingOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.ensemble import BaggingClassifier"
  modelUserFriendlyName = "Bagging"
}

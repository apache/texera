package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRidgeOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import RidgeClassifier"
  modelUserFriendlyName = "Ridge Regression"
}

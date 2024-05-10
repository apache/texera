package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRidgeCVOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import RidgeClassifierCV"
  modelUserFriendlyName = "Ridge Regression Cross Validation"
}

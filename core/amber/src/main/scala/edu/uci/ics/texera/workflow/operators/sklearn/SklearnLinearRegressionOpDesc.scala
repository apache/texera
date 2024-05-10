package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLinearRegressionOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import LinearRegression"
  modelUserFriendlyName = "Linear Regression"
  classification = false
}

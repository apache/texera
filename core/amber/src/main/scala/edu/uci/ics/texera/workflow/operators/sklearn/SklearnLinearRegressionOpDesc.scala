package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLinearRegressionOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.linear_model import LinearRegression"
  name = "Linear Regression"
  classification = false
}

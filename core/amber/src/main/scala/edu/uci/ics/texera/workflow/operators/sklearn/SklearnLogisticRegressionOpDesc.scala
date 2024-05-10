package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLogisticRegressionOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import LogisticRegression"
  modelUserFriendlyName = "Logistic Regression"
}

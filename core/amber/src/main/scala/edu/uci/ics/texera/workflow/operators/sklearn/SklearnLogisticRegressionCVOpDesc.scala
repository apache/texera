package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLogisticRegressionCVOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import LogisticRegressionCV"
  modelUserFriendlyName = "Logistic Regression Cross Validation"
}

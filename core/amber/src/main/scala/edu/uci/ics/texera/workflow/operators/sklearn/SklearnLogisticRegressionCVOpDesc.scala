package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLogisticRegressionCVOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.linear_model import LogisticRegressionCV"
  name = "Logistic Regression Cross Validation"
}

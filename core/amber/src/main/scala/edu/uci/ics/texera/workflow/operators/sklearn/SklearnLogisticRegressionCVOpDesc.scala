package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLogisticRegressionCVOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.linear_model import LogisticRegressionCV"
  modelName = "Logistic Regression Cross Validation"
}

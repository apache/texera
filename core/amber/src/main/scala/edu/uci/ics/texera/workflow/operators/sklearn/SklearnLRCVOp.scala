package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLRCVOp extends SklearnMLOp {
  model = "from sklearn.linear_model import LogisticRegressionCV"
  name = "Logistic Regression Cross Validation"
}

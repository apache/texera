package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRRCVOp extends SklearnMLOp {
  model = "from sklearn.linear_model import RidgeClassifierCV"
  name = "Ridge Regression Cross Validation"
}

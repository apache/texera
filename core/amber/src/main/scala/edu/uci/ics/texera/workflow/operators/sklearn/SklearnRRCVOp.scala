package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRRCVOp extends SklearnMLOp {
  modelImport = "from sklearn.linear_model import RidgeClassifierCV"
  model = "RidgeClassifierCV()"
  operatorName = "Ridge Regression Cross Validation"
}

package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRROp extends SklearnMLOp {
  modelImport = "from sklearn.linear_model import RidgeClassifier"
  model = "RidgeClassifier()"
  operatorName = "Ridge Regression"
}

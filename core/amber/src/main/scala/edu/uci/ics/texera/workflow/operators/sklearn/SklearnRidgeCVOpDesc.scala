package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRidgeCVOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.linear_model import RidgeClassifierCV"
  name = "Ridge Regression Cross Validation"
}

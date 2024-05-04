package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLRCVOp extends SklearnMLOpDesc {
  model = "from sklearn.linear_model import LogisticRegressionCV"
  name = "Logistic Regression Cross Validation"
}

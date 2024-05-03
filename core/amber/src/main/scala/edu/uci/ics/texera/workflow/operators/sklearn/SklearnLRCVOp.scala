package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLRCVOp extends SklearnMLOp {
  modelImport = "from sklearn.linear_model import LogisticRegressionCV"
  model = "LogisticRegressionCV()"
  operatorName = "Logistic Regression Cross Validation"
}

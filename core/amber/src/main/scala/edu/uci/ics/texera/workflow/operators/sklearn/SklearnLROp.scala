package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLROp extends SklearnMLOp {
  modelImport = "from sklearn.linear_model import LogisticRegression"
  model = "LogisticRegression()"
  operatorName = "Logistic Regression"
}

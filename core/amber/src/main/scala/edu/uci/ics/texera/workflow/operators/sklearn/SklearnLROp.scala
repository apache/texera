package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLROp extends SklearnOp {
  override val modelImport = "from sklearn.linear_model import LogisticRegression"
  override val model = "LogisticRegression()"
  override val operatorName = "Logistic Regression"
}

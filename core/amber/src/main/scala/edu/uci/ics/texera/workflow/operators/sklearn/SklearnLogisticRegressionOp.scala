package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnLogisticRegressionOp extends SklearnOp {
  override def model: String = s"""from sklearn.linear_model import LogisticRegression
       |model = LogisticRegression()""".stripMargin
  override def operatorName: String = "Logistic Regression"
}
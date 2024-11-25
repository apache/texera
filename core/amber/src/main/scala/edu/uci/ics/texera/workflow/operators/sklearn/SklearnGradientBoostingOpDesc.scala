package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnGradientBoostingOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.ensemble import GradientBoostingClassifier"
  override def getUserFriendlyModelName = "Gradient Boosting"
}

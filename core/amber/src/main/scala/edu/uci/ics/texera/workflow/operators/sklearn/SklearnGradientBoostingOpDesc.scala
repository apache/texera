package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnGradientBoostingOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.ensemble import GradientBoostingClassifier"
  override def getUserFriendlyModelName = "Gradient Boosting"
}

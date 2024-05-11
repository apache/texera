package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRandomForestOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.ensemble import RandomForestClassifier"
  override def getUserFriendlyModelName = "Random Forest"
}

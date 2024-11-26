package edu.uci.ics.amber.operator.sklearn

class SklearnRandomForestOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.ensemble import RandomForestClassifier"
  override def getUserFriendlyModelName = "Random Forest"
}

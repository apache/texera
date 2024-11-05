package edu.uci.ics.amber.operator.sklearn

class SklearnRandomForestOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.ensemble import RandomForestClassifier"
  override def getUserFriendlyModelName = "Random Forest"
}

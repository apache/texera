package edu.uci.ics.amber.operator.sklearn

class SklearnPassiveAggressiveOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.linear_model import PassiveAggressiveClassifier"
  override def getUserFriendlyModelName = "Passive Aggressive"
}

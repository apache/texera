package edu.uci.ics.amber.operator.sklearn

class SklearnDecisionTreeOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.tree import DecisionTreeClassifier"
  override def getUserFriendlyModelName = "Decision Tree"
}

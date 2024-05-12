package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnDecisionTreeOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.tree import DecisionTreeClassifier"
  override def getUserFriendlyModelName = "Decision Tree"
}

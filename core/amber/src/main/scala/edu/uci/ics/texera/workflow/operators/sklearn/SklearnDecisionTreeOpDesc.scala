package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnDecisionTreeOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.tree import DecisionTreeClassifier"
  modelUserFriendlyName = "Decision Tree"
}

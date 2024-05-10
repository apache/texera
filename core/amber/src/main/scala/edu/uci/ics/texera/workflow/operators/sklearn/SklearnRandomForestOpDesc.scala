package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnRandomForestOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.ensemble import RandomForestClassifier"
  modelUserFriendlyName = "Random Forest"
}

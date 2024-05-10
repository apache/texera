package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnGradientBoostingOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.ensemble import GradientBoostingClassifier"
  modelUserFriendlyName = "Gradient Boosting"
}

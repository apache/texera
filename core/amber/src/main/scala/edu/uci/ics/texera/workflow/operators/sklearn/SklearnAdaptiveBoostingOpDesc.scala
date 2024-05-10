package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnAdaptiveBoostingOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.ensemble import AdaBoostClassifier"
  modelUserFriendlyName = "Adaptive Boosting"
}

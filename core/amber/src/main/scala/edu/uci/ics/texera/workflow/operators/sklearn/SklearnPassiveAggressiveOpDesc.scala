package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnPassiveAggressiveOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import PassiveAggressiveClassifier"
  modelUserFriendlyName = "Passive Aggressive"
}

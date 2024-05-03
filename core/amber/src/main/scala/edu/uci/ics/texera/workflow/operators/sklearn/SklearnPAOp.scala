package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnPAOp extends SklearnMLOp {
  modelImport = "from sklearn.linear_model import PassiveAggressiveClassifier"
  model = "PassiveAggressiveClassifier()"
  operatorName = "Passive Aggressive"
}

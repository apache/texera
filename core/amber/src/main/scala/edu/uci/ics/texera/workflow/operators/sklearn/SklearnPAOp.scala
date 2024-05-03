package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnPAOp extends SklearnMLOp {
  model = "from sklearn.linear_model import PassiveAggressiveClassifier"
  name = "Passive Aggressive"
}

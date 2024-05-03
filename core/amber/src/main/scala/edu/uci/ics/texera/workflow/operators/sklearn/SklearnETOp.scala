package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnETOp extends SklearnMLOp {
  modelImport = "from sklearn.tree import ExtraTreeClassifier"
  model = "ExtraTreeClassifier()"
  operatorName = "Extra Tree"
}

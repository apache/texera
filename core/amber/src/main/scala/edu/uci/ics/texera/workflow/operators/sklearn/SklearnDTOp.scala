package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnDTOp extends SklearnMLOp {
  modelImport = "from sklearn.tree import DecisionTreeClassifier"
  model = "DecisionTreeClassifier()"
  operatorName = "Decision Tree"
}

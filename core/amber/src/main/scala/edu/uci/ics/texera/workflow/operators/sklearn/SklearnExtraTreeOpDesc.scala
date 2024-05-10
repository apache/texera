package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnExtraTreeOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.tree import ExtraTreeClassifier"
  modelUserFriendlyName = "Extra Tree"
}

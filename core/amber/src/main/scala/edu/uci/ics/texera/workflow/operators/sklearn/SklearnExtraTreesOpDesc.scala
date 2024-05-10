package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnExtraTreesOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.ensemble import ExtraTreesClassifier"
  modelUserFriendlyName = "Extra Trees"
}

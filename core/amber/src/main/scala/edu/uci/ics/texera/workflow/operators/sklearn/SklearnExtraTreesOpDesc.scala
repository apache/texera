package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnExtraTreesOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.ensemble import ExtraTreesClassifier"
  override def getUserFriendlyModelName = "Extra Trees"
}

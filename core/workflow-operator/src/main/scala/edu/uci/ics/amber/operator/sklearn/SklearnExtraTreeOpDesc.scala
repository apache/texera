package edu.uci.ics.amber.operator.sklearn

class SklearnExtraTreeOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.tree import ExtraTreeClassifier"
  override def getUserFriendlyModelName = "Extra Tree"
}

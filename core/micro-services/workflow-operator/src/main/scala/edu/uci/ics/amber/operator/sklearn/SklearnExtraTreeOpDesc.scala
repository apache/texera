package edu.uci.ics.amber.operator.sklearn

class SklearnExtraTreeOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.tree import ExtraTreeClassifier"
  override def getUserFriendlyModelName = "Extra Tree"
}

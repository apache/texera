package edu.uci.ics.amber.operator.sklearn

class SklearnExtraTreesOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.ensemble import ExtraTreesClassifier"
  override def getUserFriendlyModelName = "Extra Trees"
}

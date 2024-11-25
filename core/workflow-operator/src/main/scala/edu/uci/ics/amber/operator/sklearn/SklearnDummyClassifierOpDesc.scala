package edu.uci.ics.amber.operator.sklearn

class SklearnDummyClassifierOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.dummy import dummy"
  override def getUserFriendlyModelName = "Dummy Classifier"
}

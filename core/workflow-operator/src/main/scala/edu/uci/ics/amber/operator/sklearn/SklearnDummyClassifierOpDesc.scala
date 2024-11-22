package edu.uci.ics.amber.operator.sklearn

class SklearnDummyClassifierOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.dummy import dummy"
  override def getUserFriendlyModelName = "Dummy Classifier"
}

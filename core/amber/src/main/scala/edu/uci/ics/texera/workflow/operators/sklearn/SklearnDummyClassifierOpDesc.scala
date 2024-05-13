package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnDummyClassifierOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.dummy import dummy"
  override def getUserFriendlyModelName = "Dummy Classifier"
}

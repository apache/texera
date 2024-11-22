package edu.uci.ics.amber.operator.sklearn

class SklearnComplementNaiveBayesOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.naive_bayes import ComplementNB"
  override def getUserFriendlyModelName = "Complement Naive Bayes"
}

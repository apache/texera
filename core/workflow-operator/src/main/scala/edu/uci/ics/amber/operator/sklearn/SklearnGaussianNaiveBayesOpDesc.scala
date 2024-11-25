package edu.uci.ics.amber.operator.sklearn

class SklearnGaussianNaiveBayesOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.naive_bayes import GaussianNB"
  override def getUserFriendlyModelName = "Gaussian Naive Bayes"
}

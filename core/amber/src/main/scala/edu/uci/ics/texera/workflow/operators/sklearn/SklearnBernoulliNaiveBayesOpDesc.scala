package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnBernoulliNaiveBayesOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.naive_bayes import BernoulliNB"
  override def getUserFriendlyModelName = "Bernoulli Naive Bayes"
}

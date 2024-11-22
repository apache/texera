package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnMultinomialNaiveBayesOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.naive_bayes import MultinomialNB"
  override def getUserFriendlyModelName = "Multinomial Naive Bayes"
}

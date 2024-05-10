package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnMultinomialNaiveBayesOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.naive_bayes import MultinomialNB"
  modelUserFriendlyName = "Multinomial Naive Bayes"
}

package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnBernoulliNaiveBayesOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.naive_bayes import BernoulliNB"
  modelUserFriendlyName = "Bernoulli Naive Bayes"
}

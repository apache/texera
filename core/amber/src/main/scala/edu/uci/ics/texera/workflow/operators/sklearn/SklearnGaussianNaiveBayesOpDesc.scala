package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnGaussianNaiveBayesOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.naive_bayes import GaussianNB"
  modelUserFriendlyName = "Gaussian Naive Bayes"
}

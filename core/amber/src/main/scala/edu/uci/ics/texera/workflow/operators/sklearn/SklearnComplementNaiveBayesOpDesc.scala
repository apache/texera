package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnComplementNaiveBayesOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.naive_bayes import ComplementNB"
  modelUserFriendlyName = "Complement Naive Bayes"
}

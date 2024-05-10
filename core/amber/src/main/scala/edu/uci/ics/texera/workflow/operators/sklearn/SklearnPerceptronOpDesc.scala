package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnPerceptronOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.linear_model import Perceptron"
  modelUserFriendlyName = "Linear Perceptron"
}

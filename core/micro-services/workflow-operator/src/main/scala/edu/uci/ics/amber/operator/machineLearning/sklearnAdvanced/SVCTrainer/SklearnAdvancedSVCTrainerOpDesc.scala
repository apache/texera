package edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer

import edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor

class SklearnAdvancedSVCTrainerOpDesc
    extends SklearnMLOperatorDescriptor[SklearnAdvancedSVCParameters] {
  override def getImportStatements: String = {
    "from sklearn.svm import SVC"
  }

  override def getOperatorInfo: String = {
    "SVM Classifier"
  }
}

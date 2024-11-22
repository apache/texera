package edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer

import edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor

class SklearnAdvancedSVRTrainerOpDesc
    extends SklearnMLOperatorDescriptor[SklearnAdvancedSVRParameters] {
  override def getImportStatements: String = {
    "from sklearn.svm import SVR"
  }

  override def getOperatorInfo: String = {
    "SVM Regressor"
  }
}

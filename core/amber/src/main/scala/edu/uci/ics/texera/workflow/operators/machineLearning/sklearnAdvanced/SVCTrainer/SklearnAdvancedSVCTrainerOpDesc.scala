package edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.SVCTrainer

import edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor

class SklearnAdvancedSVCTrainerOpDesc
    extends SklearnMLOperatorDescriptor[SklearnAdvancedSVCParameters] {
  override def getImportStatements: String = {
    "from sklearn.svm import SVC"
  }

  override def getOperatorInfo: String = {
    "SVM Classifier"
  }
}

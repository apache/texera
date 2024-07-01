package edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.SVRTrainer.SVCTrainer

import edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.SVRTrainer.SklearnAdvancedSVRParameters
import edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor

class SklearnAdvancedSVRTrainerOpDesc
    extends SklearnMLOperatorDescriptor[SklearnAdvancedSVRParameters] {
  override def getImportStatements: String = {
    "from sklearn.svm import SVR"
  }

  override def getOperatorInfo: String = {
    "SVM Regressor"
  }
}

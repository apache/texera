package edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.KNNTrainer

import edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor

class SklearnAdvancedKNNRegressorTrainerOpDesc
    extends SklearnMLOperatorDescriptor[SklearnAdvancedKNNParameters] {
  override def getImportStatements: String = {
    "from sklearn.neighbors import KNeighborsRegressor"
  }

  override def getOperatorInfo: String = {
    "KNN Regressor"
  }
}

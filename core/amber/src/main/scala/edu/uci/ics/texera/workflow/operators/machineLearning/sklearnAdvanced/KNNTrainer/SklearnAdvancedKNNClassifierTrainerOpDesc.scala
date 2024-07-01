package edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.KNNTrainer

import edu.uci.ics.texera.workflow.operators.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor

class SklearnAdvancedKNNClassifierTrainerOpDesc
    extends SklearnMLOperatorDescriptor[SklearnAdvancedKNNParameters] {
  override def getImportStatements: String = {
    "from sklearn.neighbors import KNeighborsClassifier"
  }

  override def getOperatorInfo: String = {
    "KNN Classifier"
  }
}

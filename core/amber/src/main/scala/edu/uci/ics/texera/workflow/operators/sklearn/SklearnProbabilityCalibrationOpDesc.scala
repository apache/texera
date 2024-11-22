package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnProbabilityCalibrationOpDesc extends SklearnClassifierOpDesc {
  override def getImportStatements = "from sklearn.calibration import CalibratedClassifierCV"
  override def getUserFriendlyModelName = "Probability Calibration"
}

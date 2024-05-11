package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnProbabilityCalibrationOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.calibration import CalibratedClassifierCV"
  override def getUserFriendlyModelName = "Probability Calibration"
}

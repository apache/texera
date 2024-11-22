package edu.uci.ics.amber.operator.sklearn

class SklearnProbabilityCalibrationOpDesc extends SklearnMLOpDesc {
  override def getImportStatements = "from sklearn.calibration import CalibratedClassifierCV"
  override def getUserFriendlyModelName = "Probability Calibration"
}

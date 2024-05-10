package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnProbabilityCalibrationOpDesc extends SklearnMLOpDesc {
  modelImportStatement = "from sklearn.calibration import CalibratedClassifierCV"
  modelUserFriendlyName = "Probability Calibration"
}

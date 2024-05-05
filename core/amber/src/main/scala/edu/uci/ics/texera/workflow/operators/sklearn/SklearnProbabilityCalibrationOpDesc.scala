package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnProbabilityCalibrationOpDesc extends SklearnMLOpDesc {
  model = "from sklearn.calibration import CalibratedClassifierCV"
  name = "Probability Calibration"
}

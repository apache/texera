package edu.uci.ics.texera.workflow.operators.sklearn

class SklearnPCOp extends SklearnMLOp {
  model = "from sklearn.calibration import CalibratedClassifierCV"
  name = "Probability Calibration"
}

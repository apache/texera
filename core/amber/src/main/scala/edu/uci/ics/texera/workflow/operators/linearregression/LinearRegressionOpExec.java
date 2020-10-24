package edu.uci.ics.texera.workflow.operators.linearregression;

import edu.uci.ics.texera.workflow.common.operators.mlmodel.MLModelOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.collection.immutable.List;

public class LinearRegressionOpExec extends MLModelOpExec{

  private String xAttr;
  private String yAttr;

  private double learningRate = 0.1;
  private double b_current = 0;
  private double w_current = 0;

  private Double[] results = null;
  private double w_gradient = 0;
  private double b_gradient = 0;

  LinearRegressionOpExec(String xAttr, String yAttr, double learningRate){
    this.xAttr = xAttr;
    this.yAttr = yAttr;
    this.learningRate = learningRate;
  }

  @Override
  public int getTotalEpochsCount() {
    return 100;
  }

  @Override
  public void predict(Tuple[] minibatch) {
    results = new Double[minibatch.length];

    int tIdx = 0;
    for(Tuple t: minibatch) {
      Double x = Double.valueOf(t.getField(xAttr));
      results[tIdx] = (w_current * x) + b_current;
      tIdx++;
    }
  }

  @Override
  public void calculateLossGradient(Tuple[] minibatch) {
    double n = minibatch.length * 1.0;
    w_gradient = 0;
    b_gradient = 0;
    int tIdx = 0;
    for(Double result: results) {
      Double x = Double.valueOf(minibatch[tIdx].getField(xAttr));
      Double y = Double.valueOf(minibatch[tIdx].getField(yAttr));
      w_gradient += x * (y - result);
      b_gradient += (y - result);
      tIdx++;
    }
    w_gradient = (-2.0/n) * Math.round(w_gradient*100.0)/100.0;
    b_gradient = (-2.0/n) * Math.round(b_gradient*100.0)/100.0;
    w_gradient = Math.round(w_gradient*100.0)/100.0;
    b_gradient = Math.round(b_gradient*100.0)/100.0;
  }

  @Override
  public void readjustWeight() {
    w_current = w_current - (learningRate * w_gradient);
    b_current = b_current - (learningRate * b_gradient);
    w_current = Math.round(w_current*100.0)/100.0;
    b_current = Math.round(b_current*100.0)/100.0;

    System.out.println("Epoch "+ currentEpoch() + " Learning Rate " + learningRate + ", Current w and b values are : " + w_current + " " + b_current);
  }
}

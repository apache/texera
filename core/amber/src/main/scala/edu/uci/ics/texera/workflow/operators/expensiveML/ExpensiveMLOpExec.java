package edu.uci.ics.texera.workflow.operators.expensiveML;


import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import org.apache.commons.lang3.ArrayUtils;
import org.deeplearning4j.nn.conf.GradientNormalization;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nadam;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import scala.Function1;
import scala.Serializable;

public class ExpensiveMLOpExec extends MapOpExec {

    private final ExpensiveMLOpDesc opDesc;
    private final OperatorSchemaInfo operatorSchemaInfo;
    public MultiLayerNetwork net = null;
    public int currentInputNum = 10;
    public int modelScaleFactor = 16;
    public int featureSize = 1;

    public ExpensiveMLOpExec(ExpensiveMLOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        this.setMapFunc(
                // must cast the lambda function to "(Function & Serializable)" in Java
                (Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public INDArray mkInput(Double prev_trans) {
        return Nd4j.create(
                ArrayUtils.addAll(new double[]{1.3, 2.2, 1.3, 2.2, 1.3, 2.2, 3.3, 1.3, 2.2}, new double[]{prev_trans}),
                new int[]{1, 1, currentInputNum});
    }

    void buildRNN() {
        // currentInputNum = numInputEvts;
        NeuralNetConfiguration.ListBuilder confBuilder = new NeuralNetConfiguration.Builder()
                .seed(256)
                .weightInit(WeightInit.XAVIER)
                .updater(new Nadam())
                .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)  //Not always required, but helps with this data set
                .gradientNormalizationThreshold(0.5)
                .list().layer(new LSTM.Builder().activation(Activation.TANH).nIn(featureSize).nOut(modelScaleFactor).build());
        int originalFactor = modelScaleFactor;
        while (modelScaleFactor > 8) {
            confBuilder = confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(modelScaleFactor).nOut(modelScaleFactor / 2).build());
            modelScaleFactor /= 2;
        }
        while (modelScaleFactor < originalFactor) {
            confBuilder = confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(modelScaleFactor).nOut(modelScaleFactor * 2).build());
            modelScaleFactor *= 2;
        }
        confBuilder = confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(modelScaleFactor).nOut(16).build());
        MultiLayerConfiguration conf = confBuilder.layer(new RnnOutputLayer.Builder(
                        LossFunctions.LossFunction.MCXENT)
                        .activation(Activation.SOFTMAX).nIn(16).nOut(2).build())
                .build();
        if (net != null) {
            net.clear();
            net.close();
        }
        net = new MultiLayerNetwork(conf);
        net.init();
    }

    public Tuple processTuple(Tuple t) {
        if (net == null) {
            buildRNN();
        }
        Double val = Double.parseDouble(t.getField(opDesc.attribute).toString());
        INDArray output = net.output(mkInput(val));
        double fraudProb = output.getDouble(0, 0, currentInputNum - 1);
        return Tuple.newBuilder(operatorSchemaInfo.outputSchema()).add(t).add(opDesc.resultAttribute, AttributeType.DOUBLE, fraudProb).build();
    }

}

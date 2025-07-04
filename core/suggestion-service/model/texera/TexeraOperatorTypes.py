class TexeraOperatorTypes:
    class DataInput:
        CSVFileScan = "CSVFileScan"
        FileScan = "FileScan"
        JSONLFileScan = "JSONLFileScan"
        TextInput = "TextInput"
        ParallelCSVFileScan = "ParallelCSVFileScan"

    class MachineLearning:
        class SkLearn:
            SklearnLogisticRegression = "SklearnLogisticRegression"
            SklearnLogisticRegressionCV = "SklearnLogisticRegressionCV"
            SklearnRidge = "SklearnRidge"
            SklearnRidgeCV = "SklearnRidgeCV"
            SklearnSDG = "SklearnSDG"
            SklearnPassiveAggressive = "SklearnPassiveAggressive"
            SklearnPerceptron = "SklearnPerceptron"
            SklearnKNN = "SklearnKNN"
            SklearnNearestCentroid = "SklearnNearestCentroid"
            SklearnSVM = "SklearnSVM"
            SklearnLinearSVM = "SklearnLinearSVM"
            SklearnLinearRegression = "SklearnLinearRegression"
            SklearnDecisionTree = "SklearnDecisionTree"
            SklearnExtraTree = "SklearnExtraTree"
            SklearnMultiLayerPerceptron = "SklearnMultiLayerPerceptron"
            SklearnProbabilityCalibration = "SklearnProbabilityCalibration"
            SklearnRandomForest = "SklearnRandomForest"
            SklearnBagging = "SklearnBagging"
            SklearnGradientBoosting = "SklearnGradientBoosting"
            SklearnAdaptiveBoosting = "SklearnAdaptiveBoosting"
            SklearnExtraTrees = "SklearnExtraTrees"
            SklearnGaussianNaiveBayes = "SklearnGaussianNaiveBayes"
            SklearnMultinomialNaiveBayes = "SklearnMultinomialNaiveBayes"
            SklearnComplementNaiveBayes = "SklearnComplementNaiveBayes"
            SklearnBernoulliNaiveBayes = "SklearnBernoulliNaiveBayes"
            SklearnDummyClassifier = "SklearnDummyClassifier"
            SklearnPrediction = "SklearnPrediction"

        class AdvancedSkLearn:
            KNNClassifierTrainer = "KNNClassifierTrainer"
            KNNRegressorTrainer = "KNNRegressorTrainer"
            SVCTrainer = "SVCTrainer"
            SVRTrainer = "SVRTrainer"

        class HuggingFace:
            HuggingFaceSentimentAnalysis = "HuggingFaceSentimentAnalysis"
            HuggingFaceTextSummarization = "HuggingFaceTextSummarization"
            HuggingFaceSpamSMSDetection = "HuggingFaceSpamSMSDetection"
            HuggingFaceIrisLogisticRegression = "HuggingFaceIrisLogisticRegression"

        class General:
            Scorer = "Scorer"
            Split = "Split"
            SentimentAnalysis = "SentimentAnalysis"

    class UDF:
        PythonUDFV2 = "PythonUDFV2"
        PythonUDFSourceV2 = "PythonUDFSourceV2"
        DualInputPortsPythonUDFV2 = "DualInputPortsPythonUDFV2"
        PythonLambdaFunction = "PythonLambdaFunction"
        PythonTableReducer = "PythonTableReducer"

        JavaUDF = "JavaUDF"
        RUDF = "RUDF"
        RUDFSource = "RUDFSource"


def get_flat_list(cls):
    result = []
    for attr_name in dir(cls):
        attr_value = getattr(cls, attr_name)
        if not attr_name.startswith("__") and not callable(attr_value):
            if isinstance(attr_value, type):
                result.extend(get_flat_list(attr_value))
            else:
                result.append(attr_value)
    return result

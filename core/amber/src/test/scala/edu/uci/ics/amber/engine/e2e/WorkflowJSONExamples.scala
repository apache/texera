package edu.uci.ics.amber.engine.e2e

object WorkflowJSONExamples {

  val headerlessCsvToSink =
    """{"type":"ExecuteWorkflowRequest",
      |"operators":[
      |{"delimiter":",","header":true,"file path":"src/test/resources/CountrySalesData.csv","operatorID":"LocalCsvFileScan-operator","operatorType":"LocalCsvFileScan"},
      |{"operatorID":"SimpleSink-operator","operatorType":"SimpleSink"}],
      |"links":[
      |{"origin":"LocalCsvFileScan-operator","destination":"SimpleSink-operator"}],
      |"breakpoints":[]
      |}""".stripMargin

  val headerlessCsvToKeywordToSink =
    """{"type":"ExecuteWorkflowRequest",
      |"operators":[
      |{"delimiter":",","header":false,"file path":"src/test/resources/CountrySalesDataHeaderless.csv","operatorID":"LocalCsvFileScan-operator","operatorType":"LocalCsvFileScan"},
      |{"attribute":"column0","keyword":"Asia","operatorID":"KeywordSearch-operator","operatorType":"KeywordSearch"},
      |{"operatorID":"SimpleSink-operator","operatorType":"SimpleSink"}],
      |"links":[
      |{"origin":"LocalCsvFileScan-operator","destination":"KeywordSearch-operator"},
      |{"origin":"KeywordSearch-operator","destination":"SimpleSink-operator"}],
      |"breakpoints":[]}""".stripMargin

  val csvToSink =
    """{"type":"ExecuteWorkflowRequest",
      |"operators":[
      |{"delimiter":",","header":true,"file path":"src/test/resources/CountrySalesData.csv","operatorID":"LocalCsvFileScan-operator","operatorType":"LocalCsvFileScan"},
      |{"operatorID":"SimpleSink-operator","operatorType":"SimpleSink"}],
      |"links":[
      |{"origin":"LocalCsvFileScan-operator","destination":"SimpleSink-operator"}],
      |"breakpoints":[]
      |}""".stripMargin

  val csvToKeywordToSink =
    """{"type":"ExecuteWorkflowRequest",
      |"operators":[
      |{"delimiter":",","header":true,"file path":"src/test/resources/CountrySalesData.csv","operatorID":"LocalCsvFileScan-operator","operatorType":"LocalCsvFileScan"},
      |{"attribute":"Region","keyword":"Asia","operatorID":"KeywordSearch-operator","operatorType":"KeywordSearch"},
      |{"operatorID":"SimpleSink-operator","operatorType":"SimpleSink"}],
      |"links":[
      |{"origin":"LocalCsvFileScan-operator","destination":"KeywordSearch-operator"},
      |{"origin":"KeywordSearch-operator","destination":"SimpleSink-operator"}],
      |"breakpoints":[]}""".stripMargin

  val csvToKeywordToCountToSink =
    """{"type":"ExecuteWorkflowRequest",
      |"operators":[
      |{"delimiter":",","header":true,"file path":"src/test/resources/CountrySalesData.csv","operatorID":"LocalCsvFileScan-operator","operatorType":"LocalCsvFileScan"},
      |{"attribute":"Region","keyword":"Asia","operatorID":"KeywordSearch-operator","operatorType":"KeywordSearch"},
      |{"groupByKeys":[],"aggFunction":"count","attribute":"Region","result attribute":"count_rows","operatorID":"Aggregate-operator","operatorType":"Aggregate"},
      |{"operatorID":"SimpleSink-operator","operatorType":"SimpleSink"}],
      |"links":[
      |{"origin":"LocalCsvFileScan-operator","destination":"KeywordSearch-operator"},
      |{"origin":"KeywordSearch-operator","destination":"Aggregate-operator"},
      |{"origin":"Aggregate-operator","destination":"SimpleSink-operator"}],
      |"breakpoints":[]}""".stripMargin
}

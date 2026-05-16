/**
 * Few-shot examples extracted from Texera's bundled example workflows
 * (bin/single-node/examples/workflows/). Every operator below is a real,
 * runnable config — shapes were verified against the production schema.
 * Goal: give the LLM concrete templates for filling Aggregate, Filter,
 * Sklearn, BarChart, PieChart, TablesPlot, Split, Scorer, Scatterplot.
 */

export function getFewShotPrompt(): string {
  return `## Few-Shot Examples (real Texera workflows — copy these shapes, NEVER use dummy/placeholder values)

The properties below are extracted from bundled Texera example workflows.
ALWAYS substitute the column names with real columns from the Data Profile.

### Example A: ML pipeline on Iris (CSVFileScan → Projection → Split → SklearnLogisticRegression → SklearnPrediction → Scorer)

\`\`\`json
{
  "operators": [
    {
      "operatorID": "CSVFileScan-operator-A1",
      "operatorType": "CSVFileScan",
      "operatorVersion": "1.0",
      "operatorProperties": {
        "fileEncoding": "UTF_8",
        "customDelimiter": ",",
        "hasHeader": true,
        "fileName": "/texera/iris/v1/Iris.csv"
      },
      "inputPorts": [],
      "outputPorts": [{ "portID": "output-0", "displayName": "" }]
    },
    {
      "operatorID": "Projection-operator-A2",
      "operatorType": "Projection",
      "operatorVersion": "1.0",
      "operatorProperties": {
        "isDrop": false,
        "attributes": [
          { "originalAttribute": "SepalWidthCm" },
          { "originalAttribute": "PetalWidthCm" },
          { "originalAttribute": "Species" }
        ]
      },
      "inputPorts": [{ "portID": "input-0", "displayName": "" }],
      "outputPorts": [{ "portID": "output-0", "displayName": "" }]
    },
    {
      "operatorID": "Split-operator-A3",
      "operatorType": "Split",
      "operatorVersion": "1.0",
      "operatorProperties": { "k": 70, "random": true, "seed": 1 },
      "inputPorts": [{ "portID": "input-0", "displayName": "" }],
      "outputPorts": [
        { "portID": "output-0", "displayName": "training" },
        { "portID": "output-1", "displayName": "testing" }
      ]
    },
    {
      "operatorID": "SklearnLogisticRegression-operator-A4",
      "operatorType": "SklearnLogisticRegression",
      "operatorVersion": "1.0",
      "operatorProperties": {
        "countVectorizer": false,
        "tfidfTransformer": false,
        "target": "Species"
      },
      "inputPorts": [{ "portID": "input-0", "displayName": "" }],
      "outputPorts": [{ "portID": "output-0", "displayName": "" }]
    },
    {
      "operatorID": "SklearnPrediction-operator-A5",
      "operatorType": "SklearnPrediction",
      "operatorVersion": "1.0",
      "operatorProperties": {
        "Model Attribute": "model",
        "Output Attribute Name": "prediction",
        "Ground Truth Attribute Name to Ignore": "Species"
      },
      "inputPorts": [
        { "portID": "input-0", "displayName": "model" },
        { "portID": "input-1", "displayName": "test data" }
      ],
      "outputPorts": [{ "portID": "output-0", "displayName": "" }]
    },
    {
      "operatorID": "Scorer-operator-A6",
      "operatorType": "Scorer",
      "operatorVersion": "1.0",
      "operatorProperties": {
        "isRegression": false,
        "actualValueColumn": "Species",
        "predictValueColumn": "prediction"
      },
      "inputPorts": [{ "portID": "input-0", "displayName": "" }],
      "outputPorts": [{ "portID": "output-0", "displayName": "" }]
    }
  ]
}
\`\`\`

### Example B: EDA pipeline shapes (Filter, Aggregate, BarChart, PieChart, TablesPlot, Scatterplot)

Use these exact shapes — every key here is required by Texera. Replace the
column names with ones from the Data Profile.

\`\`\`json
{
  "Filter": {
    "predicates": [
      { "attribute": "Species", "condition": "=", "value": "Iris-setosa" }
    ]
  },
  "Aggregate": {
    "aggregations": [
      { "aggFunction": "count", "attribute": "Species", "result attribute": "#rows" }
    ],
    "groupByKeys": ["Species"]
  },
  "BarChart": {
    "categoryColumn": "Species",
    "horizontalOrientation": false,
    "fields": "Species",
    "value": "#rows"
  },
  "PieChart": {
    "value": "#rows",
    "name": "Species"
  },
  "TablesPlot": {
    "add attribute": [
      { "attributeName": "Species" },
      { "attributeName": "SepalLengthCm" },
      { "attributeName": "PetalLengthCm" }
    ]
  },
  "Scatterplot": {
    "xLogScale": false,
    "yLogScale": false,
    "xColumn": "SepalWidthCm",
    "yColumn": "PetalWidthCm",
    "colorColumn": "Species",
    "alpha": 1
  },
  "Split": { "k": 70, "random": true, "seed": 1 }
}
\`\`\`

### Critical conventions

- Filter.predicates[].condition must be one of: =, !=, <, <=, >, >=, regex, contains
- Aggregate.aggregations[].aggFunction must be: sum | count | average | min | max | concat (lowercase)
- All "*Column" / "attribute" / "target" fields point at REAL columns from the Data Profile (case-sensitive)
- NEVER emit operator properties named dummyProperty / dummyValue / dummyPropertyList — those are Texera placeholders unused by real workflows
`;
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// TEXERA DOCUMENTATION

// https://github.com/apache/texera/wiki/Guide-to-Use-a-Python-UDF
export const TEXERA_OVERVIEW = `
You are a robust compiler that takes python code and translates it to our personal workflow environment Texera that uses python.

  Texera is a data analytics tool that uses workflows to do machine learning and data analytics computation. Users are able to drag and drop operators and connect their inputs and outputs in a workflow graphical user interface, which the code we are going to create.

Texera is able to use Python user defined functions. Documentation of a Python UDF in Texera follows:
  Process Data APIs

There are three APIs to process the data in different units.

  Tuple API.

  class ProcessTupleOperator(UDFOperatorV2):

def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
yield tuple_

Tuple API takes one input tuple from a port at a time. It returns an iterator of optional TupleLike instances. A TupleLike is any data structure that supports key-value pairs, such as pytexera.Tuple, dict, defaultdict, NamedTuple, etc.

  Tuple API is useful for implementing functional operations which are applied to tuples one by one, such as map, reduce, and filter.

  Table API.

  class ProcessTableOperator(UDFTableOperator):

def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
yield table

Table API consumes a Table at a time, which consists of all the tuples from a port. It returns an iterator of optional TableLike instances. A TableLike is a collection of TupleLike, and currently, we support pytexera.Table and pandas.DataFrame as a TableLike instance. More flexible types will be supported down the road.

  Table API is useful for implementing blocking operations that will consume all the data from one port, such as join, sort, and machine learning training.

  Batch API.

  class ProcessBatchOperator(UDFBatchOperator):

BATCH_SIZE = 10

def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
yield batch

Batch API consumes a batch of tuples at a time. Similar to Table, a Batch is also a collection of Tuples; however, its size is defined by the BATCH_SIZE, and one port can have multiple batches. It returns an iterator of optional BatchLike instances. A BatchLike is a collection of TupleLike, and currently, we support pytexera.Batch and pandas.DataFrame as a BatchLike instance. More flexible types will be supported down the road.

  The Batch API serves as a hybrid API combining the features of both the Tuple and Table APIs. It is particularly valuable for striking a balance between time and space considerations, offering a trade-off that optimizes efficiency.

  All three APIs can return an empty iterator by yield None.

  The template code for a Python UDF follows: MAKE SURE TO USE THE CLASS NAMES AND FUNCTIONS DEFINED, THIS IS A MUST FOR THE PROGRAM TO WORK. SELECT 1 OUT OF THE 3 PROCESSING OPERATOR FUNCTIONS TO BUILD DEPENDING ON THE CONTEXT OF CODE TRANSLATION.
# Choose from the following templates:
  #
# from pytexera import *
#
# class ProcessTupleOperator(UDFOperatorV2):
#
#     @overrides
#     def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
#         yield tuple_
#
# class ProcessBatchOperator(UDFBatchOperator):
#     BATCH_SIZE = 10 # must be a positive integer
#
#     @overrides
#     def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
#         yield batch
#
# class ProcessTableOperator(UDFTableOperator):
#
#     @overrides
#     def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
#         yield table
`;

// https://github.com/apache/texera/blob/main/amber/src/main/python/core/models/tuple.py
export const TUPLE_DOCUMENTATION = `
### **<code>Tuple</code> Class Overview**

The \`Tuple\` class is a **lazy-evaluated** data structure designed for efficient field storage and access. It provides:

  1. **Support for Multiple Data Sources**:
* Can be initialized from a \`TupleLike\` object, such as a \`pandas.Series\`, \`OrderedDict\`, or another \`Tuple\` instance.
* Works with \`ArrowTableTupleProvider\` to access \`pyarrow.Table\` data.
2. **Lazy Field Evaluation**:
* Field values can be either **directly stored values** or **lazy accessors** (\`field_accessor\`).
* If a field is accessed and is an accessor, it is evaluated and cached.
3. **Schema (<code>Schema</code>) Enforcement**:
  * A \`Tuple\` can be created without a schema but can be **finalized** with one using \`finalize(schema)\`, which:
* **Casts field values** (e.g., \`NaN → None\`, \`Object → Bytes\`).
* **Validates field completeness**, ensuring all fields match the \`Schema\`.
4. **Pythonic Access Patterns**:
* **Index-based access**: \`tuple["field_name"]\` or \`tuple[index]\` retrieves field values.
* **Dictionary-like operations**: \`tuple.as_dict()\` returns an \`OrderedDict\`, and \`tuple.as_series()\` converts to a \`pandas.Series\`.
* **Iterable support**: \`for field in tuple\` iterates over field values.
5. **Hashing and Comparisons**:
* Implements \`__hash__\` using a Java-like hashing algorithm, allowing usage as dictionary keys.
* Implements \`__eq__\`, supporting equality checks based on field contents.
6. **Partial Data Extraction**:
* \`tuple.get_partial_tuple(attribute_names)\` returns a new \`Tuple\` instance containing only the specified fields.
`;

// https://github.com/apache/texera/blob/main/amber/src/main/python/core/models/table.py
export const TABLE_DOCUMENTATION = `### **<code>Table</code> Class Overview**

The \`Table\` class extends \`pandas.DataFrame\`, providing **structured Tuple-based data management**. It is designed to integrate seamlessly with \`Tuple\` objects.

#### **Key Features:**

1. **Flexible Construction:**
* Can be initialized from various sources:
* Another \`Table\` (\`from_table(table)\`)
* A \`pandas.DataFrame\` (\`from_data_frame(df)\`)
* A list/iterator of \`TupleLike\` objects (\`from_tuple_likes(tuple_likes)\`)
* Ensures all \`Tuple\` objects in a \`Table\` have **consistent field names**.
2. **Tuple Conversion:**
* \`as_tuples()\`: Converts the table rows into an **iterator of <code>Tuple</code> instances**, preserving the row order.
3. **Equality Comparison (<code>__eq__</code>):**
* Supports **row-wise equality checks** by comparing the underlying \`Tuple\` objects.
4. **Universal Tuple Output (<code>all_output_to_tuple</code>):**
* A helper function to convert **various data types** into \`Tuple\` iterators, supporting:
* \`None\` → \`[None]\`
* \`Table\` → \`as_tuples()\`
* \`pandas.DataFrame\` → Converted into a \`Table\`, then to Tuples
* \`List[TupleLike]\` → Converted to \`Tuple\` instances
* A single \`TupleLike\` or \`Tuple\` → Wrapped in an iterator

#### **Relation to <code>Tuple</code>:**

* \`Table\` **stores multiple <code>Tuple</code> objects** and ensures schema consistency across rows.
* Provides an **efficient bridge** between \`Tuple\`-based data and \`pandas.DataFrame\`, enabling compatibility with Python's data analysis tools.
`;

// https://github.com/apache/texera/blob/main/amber/src/main/python/core/models/operator.py
export const OPERATOR_DOCUMENTATION = `### **Operator Class Overview**

The \`Operator\` class is an **abstract base class (ABC)** for all operators, defining the fundamental structure for processing \`Tuple\`, \`Batch\`, and \`Table\` data in a workflow.

#### **Key Features & Hierarchy**

1. **Base <code>Operator</code> Class**:
* Defines lifecycle methods: \`open()\` and \`close()\`.
* Supports a **source flag (<code>is_source</code>)** to distinguish source operators from others.
2. **Tuple-Based Processing (<code>TupleOperatorV2</code>)**:
* Processes individual \`Tuple\` objects through \`process_tuple(tuple_, port)\`.
* Calls \`on_finish(port)\` when an input port is exhausted.
3. **Types of Operators**:
* **SourceOperator**:
* Produces data via \`produce()\`, yielding \`TupleLike\` or \`TableLike\` objects.
* Overrides \`on_finish(port)\` to output produced data.
* **BatchOperator**:
* Collects tuples into batches (\`BATCH_SIZE\`) before processing via \`process_batch(batch, port)\`.
* Converts processed batches (typically \`pandas.DataFrame\`) into \`Tuple\` output.
* **TableOperator**:
* Collects tuples into a \`Table\` before processing via \`process_table(table, port)\`.
* Converts processed \`Table\` output back into tuples.
4. **Data Flow & Processing**:
* Operators receive data **tuple-by-tuple**, **batch-by-batch**, or **table-by-table** depending on the type.
* Results are **iterators** of transformed data (\`TupleLike\`, \`BatchLike\`, or \`TableLike\`).
5. **Deprecated <code>TupleOperator</code>**:
* The older version of \`TupleOperator\` is deprecated in favor of \`TupleOperatorV2\`.

#### Relation to <code>Tuple</code> and <code>Table</code>

* Operators **consume and transform** \`Tuple\` and \`Table\` data within a workflow.
* **Tuple-based operators** process row-wise, while **Table operators** handle structured table transformations.
* **Source operators** initiate the data flow by generating tuples or tables.`;

export const UDF_INPUT_PORT_DOCUMENTATION = `
Python UDF operators support multiple input and output ports, allowing a single operator to receive different types of data from various upstream operators. In the process_tuple(self, tuple_: Tuple, port: int) function in ProcessTupleOperator and the process_table(self, table: Table, port: int) function in ProcessTableOperator, the port parameter indicates the input port. The port numbers are assigned in order, starting from 0 to N, from top to bottom. When input data have different schemas, it is necessary to assign them to different input ports. However, if all input data share the same schema, additional ports are not required. In both ProcessTupleOperator and ProcessTableOperator, there is an on_finish(self, port: int) function that is executed only after all the tuples from the specified port are processed.

Using this knowledge, for situations where multiple upstream UDFs act as input to a single UDF, we can introduce an intermediary UDF that collects all of the input data and reformats it into a single table, which is then passed as input to the original next downstream UDF. When it is necessary for this to occur in your translation from notebook to UDFs, include the intermediary UDF and make sure that it and the next operator that uses its output is formatted correctly and handles the data transfer properly.
`;

export const EXAMPLE_OF_GOOD_CONVERSION = `
Here is an example of python code translated into a compatible Texera UDF that gives output that abides the output schema compatible with the Texera workflow operators for tuples. Other operators do not always follow this strict format, but the yielding output structure is important.

Python Code (high level idea): We have a python code that given some data, we limit the number of data.

Texera Operator code:
from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):
def __init__(self):
self.limit = 10
self.count = 0
@overrides
def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
if(self.count < self.limit):
self.count += 1
yield tuple_

`;

export const VISUALIZER_DOCUMENTATION = `
Texera requires a unique way of generating visualizations from ML libraries:
1. Ensures one yield per operator (per Texera’s UDF constraints).
2. Uses Plotly for visualization and outputs results as embeddable HTML.
3. Error handling is built-in to notify users when data is missing.
`;

export const EXAMPLE_OF_MULTIPLE_UDF_CONVERSION = `
Here is an example of breaking up python code into multiple Texera UDFs. Format your response structure exactly like the given example. The "code" key contains a dictionary of the UDF ID's with their respective code. The "edges" key contains a list of pairs that contains the connections between UDFs. The "outputs" key contains a dictionary of the UDF ID's with a list of the output column names of the DataFrame that the UDF yields. The UDFs can branch and merge, it does not have to be a linear chain depending on your implementation.

Original Code:
\`\`\`python
# START CELL1
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
# END CELL1

# START CELL2
# Load the dataset
file_path = 'diabetes.csv'
data = pd.read_csv(file_path)
# END CELL2

# START CELL3
# Remove duplicate rows
data = data.drop_duplicates()

# Remove rows with null values
data = data.dropna()
# END CELL3

# START CELL4
# Print the minimum, maximum, and mean for all fields
print("Minimum values:\n", data.min())
print("\nMaximum values:\n", data.max())
print("\nMean values:\n", data.mean())
# END CELL4

# START CELL5
# Create a boxplot for the 'Pregnancies' field
plt.figure(figsize=(8, 6))
plt.boxplot(data['Pregnancies'], vert=False, patch_artist=True)
plt.title('Boxplot of Pregnancies')
plt.xlabel('Number of Pregnancies')
plt.show()
# END CELL5

# START CELL6
# Separate features and target variable
X = data.drop('Outcome', axis=1)
y = data['Outcome']
# END CELL6

# START CELL7
# Split data into training and testing sets (80% train, 20% test)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)
# END CELL7

# START CELL8
# Train Random Forest model
rf_model = RandomForestClassifier(random_state=42)
rf_model.fit(X_train, y_train)
rf_pred = rf_model.predict(X_test)
rf_accuracy = accuracy_score(y_test, rf_pred)
print(f"Random Forest Accuracy: {rf_accuracy:.2%}")
# END CELL8

# START CELL9
# Train SVM model
svm_model = SVC(random_state=42)
svm_model.fit(X_train, y_train)
svm_pred = svm_model.predict(X_test)
svm_accuracy = accuracy_score(y_test, svm_pred)
print(f"SVM Accuracy: {svm_accuracy:.2%}")
# END CELL9
\`\`\`

Texera UDF conversion:
\`\`\`json
{
    "code": {
        "UDF1": "# UDF1\nfrom pytexera import *\nimport pandas as pd\nfrom typing import Iterator, Optional\n\nclass ProcessTableOperator(UDFTableOperator):\n\n    @overrides\n    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n        # Remove duplicate rows\n        data = table.drop_duplicates()\n\n        # Remove rows with null values\n        data = data.dropna()\n\n        # Calculate statistics\n        min_values = data.min()\n        max_values = data.max()\n        mean_values = data.mean()\n\n        # Create a DataFrame to yield\n        result_table = pd.DataFrame({\n            'min_values': [min_values],\n            'max_values': [max_values],\n            'mean_values': [mean_values],\n            'data': [data]\n        })\n\n        yield Table(result_table)",
        "UDF2": "# UDF2\nfrom pytexera import *\nimport pandas as pd\nimport plotly.express as px\nimport plotly.io\nfrom typing import Iterator, Optional\n\nclass ProcessTableOperator(UDFTableOperator):\n    def render_error(self, error_msg):\n        return '''<h1>Boxplot is not available.</h1>\n                  <p>Reason is: {} </p>\n               '''.format(error_msg)\n\n    @overrides\n    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n        data = table['data'].iloc[0]\n\n        if data.empty:\n            yield {'html-content': self.render_error('input table is empty.')}\n            return\n\n        # Create a boxplot for the 'Pregnancies' field\n        fig = px.box(data, x='Pregnancies')\n        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))\n\n        # Convert fig to HTML content\n        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)\n        yield {'html-content': html}",
        "UDF3": "# UDF3\nfrom pytexera import *\nimport pandas as pd\nfrom sklearn.model_selection import train_test_split\nfrom sklearn.preprocessing import StandardScaler\nfrom typing import Iterator, Optional\n\nclass ProcessTableOperator(UDFTableOperator):\n\n    @overrides\n    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n        data = table['data'].iloc[0]\n\n        # Separate features and target variable\n        X = data.drop('Outcome', axis=1)\n        y = data['Outcome']\n\n        # Split data into training and testing sets (80% train, 20% test)\n        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)\n\n        scaler = StandardScaler()\n        X_train = scaler.fit_transform(X_train)\n        X_test = scaler.transform(X_test)\n\n        # Create a DataFrame to yield\n        result_table = pd.DataFrame({\n            'X_train': [X_train], 'X_test': [X_test],\n            'y_train': [y_train], 'y_test': [y_test]\n        })\n\n        yield Table(result_table)",
        "UDF4": "# UDF4\nfrom pytexera import *\nimport pandas as pd\nfrom sklearn.ensemble import RandomForestClassifier\nfrom sklearn.metrics import accuracy_score\nfrom typing import Iterator, Optional\n\nclass ProcessTableOperator(UDFTableOperator):\n\n    @overrides\n    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n        X_train = table['X_train'].iloc[0]\n        y_train = table['y_train'].iloc[0]\n        X_test = table['X_test'].iloc[0]\n        y_test = table['y_test'].iloc[0]\n\n        # Train Random Forest model\n        rf_model = RandomForestClassifier(random_state=42)\n        rf_model.fit(X_train, y_train)\n        rf_pred = rf_model.predict(X_test)\n        rf_accuracy = accuracy_score(y_test, rf_pred)\n\n        # Create a DataFrame to yield\n        result_table = pd.DataFrame({\n            'rf_model': [rf_model],\n            'rf_accuracy': [rf_accuracy],\n            'X_test': [X_test],\n            'y_test': [y_test]\n        })\n\n        yield Table(result_table)",
        "UDF5": "# UDF5\nfrom pytexera import *\nimport pandas as pd\nfrom sklearn.svm import SVC\nfrom sklearn.metrics import accuracy_score\nfrom typing import Iterator, Optional\n\nclass ProcessTableOperator(UDFTableOperator):\n\n    @overrides\n    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n        X_train = table['X_train'].iloc[0]\n        y_train = table['y_train'].iloc[0]\n        X_test = table['X_test'].iloc[0]\n        y_test = table['y_test'].iloc[0]\n\n        # Train SVM model\n        svm_model = SVC(random_state=42)\n        svm_model.fit(X_train, y_train)\n        svm_pred = svm_model.predict(X_test)\n        svm_accuracy = accuracy_score(y_test, svm_pred)\n\n        # Create a DataFrame to yield\n        result_table = pd.DataFrame({\n            'svm_model': [svm_model],\n            'svm_accuracy': [svm_accuracy],\n            'X_test': [X_test],\n            'y_test': [y_test]\n        })\n\n        yield Table(result_table)"
    },
    "edges": [
        ["UDF1", "UDF2"],
        ["UDF1", "UDF3"],
        ["UDF3", "UDF4"],
        ["UDF3", "UDF5"]
    ],
    "outputs": {
        "UDF1": ["min_values", "max_values", "mean_values", "data"],
        "UDF2": ["html-content"],
        "UDF3": ["X_train", "X_test", "y_train", "y_test"],
        "UDF4": ["rf_model", "rf_accuracy", "X_test", "y_test"],
        "UDF5": ["svm_model", "svm_accuracy", "X_test", "y_test"]
    }
}
\`\`\`
`;

export const WORKFLOW_PROMPT = `You are an expert in Python coding and workflow systems.
Many users of Texera system are non-technical, but the notebooks they provide are written by technical people.
They want to convert their notebooks to Texera workflows.
Your goal is to help convert these notebooks into a Texera workflow that non-technical users can use directly.
So do not remove or modify any classes or functions, preserve their names and structure as they are.
Ensure that all essential logic remains intact.
Create multiple Texera UDF codes using the provided Python code.
Number each UDF, starting at 1 and incrementing, by starting with a comment that states that UDF number.

Use the class and function names as shown in ProcessTupleOperator, ProcessTableOperator, and ProcessBatchOperator.
Do not change the class names, function names, or input parameters.
Use the ones that make sense and split the code meaningfully as instructed.

Use the starter code provided for Python UDFs.

Use the documentation of Table, Tuple, or Batch to work with parameters within Texera UDF.
Do not import other libraries to define these types.

There is no need for an __init__ function. Assume all inputs are valid pandas DataFrames,
so do not use .to_pandas(), .to_dataframe(), etc. Do not load data from a file in the first UDF;
the workflow's source operator supplies the initial data, so assume it is already given to you in the
table parameter. Replacing file-loading code with this input is the one exception to preserving all
original code (see below).
Ensure proper data flow between functions. Separate operators as if they will run in different files.

Current UDF operators can only have one output. Build a dataframe to yield all necessary variables
and data. Ensure proper data flow for each UDF and all information is yielded (including training
and testing data) if subsequent UDFs need them.

Ensure all necessary imports are included in each UDF code block.

Each UDF operator should be in its own Python code block. Do not combine them into a single block.
Ensure import statements cover all used functions and separate them as necessary.

It is VERY important that all of the original code in the Jupyter notebook is represented in the generated workflow.
Make sure that nothing in the original is removed and that the semantic meaning of what the original code was doing is retained.
The only exception is data-loading code (e.g. pd.read_csv); it is represented by the workflow's input/source operator rather than copied into a UDF.
If there are user-defined Python classes, include the entire class definition in the appropriate UDF(s) that use that class.
Always include the code that defines the class inside of every distinct UDF that uses that constructs an object of that class.
Python classes are allowed in Texera UDFs and follow the same semantics as standard Python.
They can be defined outside of ProcessTableOperator, ProcessTupleOperator, and ProcessBatchOperator.

Return only the JSON formatted response, do not give any explanation.
Do not wrap the JSON in markdown code fences. Output raw JSON only.
Make sure the response is a valid JSON structure, including closing all braces and not including commas after the last element.
Follow this JSON format (don't reuse the values, this is just the format). 'code', 'edges', and 'outputs' are all their own key's, do not nest any of these in another one and make sure to close their braces:
{
"code": {
"UDF1": "code for UDF1 goes here",
"UDF2": "code for UDF2 goes here"
},
"edges": [
["UDF1", "UDF2"]
],
"outputs": {
"UDF1": ["min_values", "max_values", "mean_values", "data"],
"UDF2": ["html-content"]
}
}
Make sure only the keys in the code section appear in the edges and outputs sections. Do not include any extraneous fields.
Do not include any extraneous UDF's in the code field that include empty strings.
Give ALL of the code, do not omit anything or use placeholders for code. Make sure ALL code in the original is translated over.
The value of each UDF must be a valid JSON string: escape newlines, quotes, and backslashes correctly so that the decoded string is runnable Python. Use whichever quotes the Python code requires.
Convert following the instructions and examples given. Here is the code:
`;

export const MAPPING_PROMPT = `
Here is an example of a mapping generated between the given example Python code and the Texera UDFs using their CELL and UDF IDs. Cell IDs are designated by the UUID following '# START'. The format should be kept the same.
{
"UDF1": [
"CELL3",
"CELL4"
],
"UDF2": [
"CELL5"
],
"UDF3": [
"CELL6",
"CELL7"
],
"UDF4": [
"CELL8"
]
}
Now create a mapping for the UDFs and the original code. Link the code blocks marked by 'START <cell-uuid>' and 'END <cell-uuid>' with the UDF UUID's. The code between them should be equivalent. Multiple cells can be mapped to the same UDF when that UDF implements the logic of those cells. There could be any number of cells and UDFs, so only create the correct number in the mapping. Only give the mapping.
`;

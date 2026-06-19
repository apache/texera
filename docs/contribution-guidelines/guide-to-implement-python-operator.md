---
title: "Guide to Implement a Python Native Operator (converting from a Python UDF)"
weight: 50
---

In the [page for PythonUDF](/docs/tutorials/guide-to-use-python-udf/), we introduced the basic concepts of PythonUDF and described each API. To let other users use the Python operators, it is necessary to implement it as a native operator.

In this section, we will discuss how to implement a Python native operator and let future users drag and drop it on the UI. We will start by implementing a sample UDF then talk about how to convert it to a native operator.

## **Starting with a Sample Python UDF**

Suppose we have a sample Python UDF named  `Treemap Visualizer`, as presented below:

<img width="548" alt="image14" src="/images/github-assets/f5d4d45c-4782-407c-b0da-07a58b7b7dfd.png">


The UDF takes a CSV file as its input. For this example, we use a dataset of geo-location information of tweets. A sample of the dataset is shown below:

<img width="685" alt="image12" src="/images/github-assets/402f9ae4-c9d0-4dff-9e35-730e808650bb.png">

The `Treemap Visualizer` UDF takes the CSV file as a table (using the Table API) and outputs an HTML page that contains a treemap figure. The HTML page will be consumed by the HTML visualizer operator, and the `View Result` operator eventually displays the figure in the browser. The visualization is presented below:

<img width="911" alt="image1" src="/images/github-assets/d2225239-fac9-4bf3-acf0-a601e3df46d7.png">

Now, let's take a closer look at the `Treemap Visualizer` UDF.
As shown in the following code block, the UDF contains 3 steps:
```python
from pytexera import *

import plotly.express as px
import plotly.io
import plotly
import numpy as np


class ProcessTableOperator(UDFTableOperator):

    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        table = table.groupby(['geo_tag.countyName','geo_tag.stateName']).size().reset_index(name='counts')
        #print(table)
        fig = px.treemap(table, path=['geo_tag.stateName','geo_tag.countyName'], values='counts',
                         color='counts', hover_data=['geo_tag.countyName','geo_tag.stateName'],
                         color_continuous_scale='RdBu',
                         color_continuous_midpoint=np.average(table['counts'], weights=table['counts']))
        fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
        yield {'html': html}
```

1. It first performs an aggregation with a groupby to calculate the number of geo_tags of each US state.
2. Then it invokes the Plotly library to create a treemap figure based on the aggregated dataset. 
3. Lastly, it converts the treemap figure object into an HTML string, by invoking the `to_html` function in the Plotly library, and yields it as the output.

## **Convert the UDF into a Python Native Operator**

Next we convert the `Treemap Visualizer` UDF into a native operator.
As described in the[page for Java native operator](/docs/contribution-guidelines/guide-to-implement-java-operator/), a native operator requires the definitions of a descriptor (Desc), an executor (Exec), and a configuration (OpConfig). A Python native operator also requires these definitions, with some unique tweaks. We use the `Treemap Visualization` operator as an example to elaborate the differences:
### Operator Descriptor (Desc)
* Operator infomation<br>
    The operator information is the same as a Java native operator, which contains the name, description, group, input port, and output port information.
* Extending interface<br>
    Instead of implementing the `OperatorDescriptor` interface, a Python native operator implements the `PythonOperatorDescriptor` interface with overriding the `generatePythonCode` method. Our example is a `VisualizationOperator`, and we need to extend it as well. 
* Python content<br>
    The `generatePythonCode` method returns the actual Python code as a string, as shown below: 

    ![wiki drawio (3)](/images/github-assets/d3804ba4-8e2f-44b5-bf15-2d69ba28fae3.png)

    Now, let's compare the code in the PythonUDF with what we write in the descriptor. As we can see, both are responsible for generating the treemap figure and converting it into an HTML page. Additionally, we've included null-value handling and error alerts to make the operator more comprehensive.
* Output schema<br>
    The Python UDF needs to define the output Schema in the property editor, while for native operators the output Schema is defined by implementing `getOutputSchema`. To do so, we use a Schema builder and add the output schema with the attribute name “html-content”.
    ```python
    override def getOutputSchema(schemas: Array[Schema]): Schema = {
            Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
          }
    ```
* Chart type<br>
    Since this operator is a visualization operator, we need to register its chart type as a `HTML_VIZ`.
    ```python
    override def chartType(): String = VisualizationConstants.HTML_VIZ
    ```
### Executor (Exec)
In all Python native operators, the executor is simply the `PythonUDFExecutor`. 
### Operator Configuration
In a Python native operator, it shares the same configuration as a Java native operator.
### Registration
It has the same process as a Java native operator.

## **Test**

After following all the steps above, you should be able to drag and drop the operator into the canvas. During the execution, the operator will output the expected result.

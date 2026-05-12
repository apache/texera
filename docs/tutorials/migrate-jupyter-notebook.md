---
title: "Migrate a Jupyter Notebook to a Texera Workflow"
weight: 60
---

This document provides guidelines on how to migrate a Jupyter notebook to a Texera workflow.

## 1. Overview
Jupyter Notebook is an open-source, browser-based environment for interactive computing that blends executable code with rich media in a single document. Work is organized into discrete cells that can be run individually, with each cell’s output persisted in the notebook.

A Texera workflow provides an operator-centric abstraction for data-science pipelines. A workflow is a directed acyclic graph (DAG) in which every node is an operator, such as CSV Scan, Projection, Filter, Aggregate, Python UDF, or ML Model, and an edge represents the flow of data between operators.

Migrating notebook code into Texera operators, then wiring those operators with links, transforms ad-hoc analyses into shareable, pipeline-oriented workflows that enable collaboration and scalable execution.



## 2. Example: convert a "tweet analysis" notebook into a workflow

> The [notebook](https://hub.texera.io/dashboard/user/dataset/124), [dataset](https://hub.texera.io/dashboard/user/dataset/124) and [workflow](https://hub.texera.io/dashboard/user/workspace/1162) in this example are available on [TexeraHub](https://hub.texera.io/dashboard/about).

### Notebook Overview
We will use a Tweet-Analysis notebook to demonstrate the migration process. The notebook has three cells:
- Cell 1
```python
import pandas as pd
import plotly.express as px

file_path = 'clean_tweets.csv'
df = pd.read_csv(file_path)
df
```
- Cell 2
```python
df_projection = df[['tweet_id', 'create_at_month']]
df_aggregated = df_projection.groupby('create_at_month').agg(**{'#tweets': ('tweet_id', 'count')}).reset_index()
df_sorted = df_aggregated.sort_values(by='create_at_month', ascending=True)
fig = px.bar(df_sorted,
             x='create_at_month',
             y='#tweets',
             color='#tweets',
             color_continuous_scale='thermal',
             labels={'create_at_month': 'Month', '#tweets': '# of Tweets'})
fig.show()
```
- Cell 3
```python
df['text_length'] = df['text'].astype(str).str.len()
length_stats = df['text_length'].agg(['min', 'max', 'mean'])
print(length_stats)
```
Below is the screenshot of the notebook after the execution:
<img width="1500" height="800" alt="Screenshot 2025-07-07 at 2 29 03 PM" src="/images/github-assets/6c821187-22f7-4f91-bf6a-f946100a964e.png" />


### 2.1. Identify the data files and upload them to a Texera dataset
From cell 1, we see the notebook reads `clean_tweets.csv`.
```python
#...
file_path = 'clean_tweets.csv'
df = pd.read_csv(file_path)
df
```

To let Texera read the same file, create a dataset in Texera, drag-and-drop the CSV file into it, and create a version:

<img width="1200" height="500" alt="Screenshot 2025-07-11 at 10 28 57 PM" src="/images/github-assets/f6913e36-0f5b-4506-8096-6c1b61af694d.png" />
<img width="1200" height="500" alt="Screenshot 2025-07-11 at 10 33 19 PM" src="/images/github-assets/49b0eeee-8aad-4683-86d8-8540e7021220.png" />




### 2.2. Read the source data using data input operators
After the file is in a dataset, create a workflow and add a data-input operator that reads the file. 

Because the file is CSV, we should use **CSVFileScanOperator** and specify the file path. Running the workflow should display the same table as Cell 1 in the result panel:
![2025-07-10 13 53 56](/images/github-assets/5efa28b6-3e72-488a-8abf-a001a5e6136d.png)



After this step, we have successfully converted cell 1 into a Texera operator.

### 2.3. Migrate data-processing logic into operators and links
#### Case 1: Use native operators for common processing logic
Cell 2 performs a sequence of operations after reading the data source: projection to keep only two columns, aggregation to calculate the number of tweets per month, sort based on count, and then visualizing using the bar chart:
```python
df_projection = df[['tweet_id', 'create_at_month']]
df_aggregated = df_projection.groupby('create_at_month').agg(**{'#tweets': ('tweet_id', 'count')}).reset_index()
df_sorted = df_aggregated.sort_values(by='create_at_month', ascending=True)
fig = px.bar(df_sorted,
             x='create_at_month',
             y='#tweets',
             color='#tweets',
             color_continuous_scale='thermal',
             labels={'create_at_month': 'Month', '#tweets': '# of Tweets'})
fig.show()
```
These operations are very common in data science pipelines. And Texera provides several native operators that have the exact same functionalities and are easy to use:
* **Projection operator** → `df[['tweet_id', 'create_at_month']]`  
* **Aggregate operator** → `groupby('create_at_month').agg(...).reset_index()`  
* **Sort operator** → `sort_values(by='create_at_month', ascending=True)`
* **Barchart operator** → `px.bar(...)`

Therefore, we can drag-n-drop these operators, connect them after the CSVFileScan. Running the workflow should display the same bar chart as in Cell 2.

![2025-07-10 13 55 12](/images/github-assets/46b0481b-f9ac-40dd-8c83-ae0a4781b397.png)

Now we have successfully migrate cell 2 into Texera.

#### Case 2: Use UDF operators for complex processing logic
According to cell 3, a new column is added to the original tweet data table to represent the length of the text column. After that, min, max, mean of the text_length column are calculated. 
```python
df['text_length'] = df['text'].astype(str).str.len()
length_stats = df['text_length'].agg(['min', 'max', 'mean'])
print(length_stats.rename({'min': 'min_len', 'max': 'max_len', 'mean': 'avg_len'}))
```

For code that involves column addition/removal and other complex data operations, Texera supports UDF operators that allow users to write custom logic as an operator that processes the data.

In this example, we can add a **PythonUDF operator** **after** the CSVScanOperator. Inside the UDF we use TableAPI as it involves the table-level column addition. Since in the `pytexera` package, Table supports most of the pandas Dataframe APIs, we can simply adjust the code in Cell 3 and put it into UDF as the processing logic. There are two ways to show the final result:
1. Use `print` statement in the UDF code block. The result will be shown in the "Console" tab:
```python
from typing import Iterator, Optional
from pytexera import *
import pandas as pd
class TextLengthStatsOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        # add a new column text_length
        table['text_length'] = table['text'].astype(str).str.len()

        # Aggregate min, max, and mean
        length_stats = table['text_length'].agg(['min', 'max', 'mean'])
        print(length_stats)
        yield None
```
<img width="1600" height="742" alt="Screenshot 2025-07-10 at 4 30 28 PM" src="/images/github-assets/77bb0de3-7e9f-4f44-ac73-1f29e34a7401.png" />

2. Yield the result as a table with columns `min`, `max`, and `mean` to the downstream. Make sure to declare the output schema in the operator panel. The result will be shown in the "Result" tab:
```python
from typing import Iterator, Optional
from pytexera import *
import pandas as pd
class TextLengthStatsOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        # add a new column text_length
        table['text_length'] = table['text'].astype(str).str.len()

        # Aggregate min, max, and mean
        length_stats = table['text_length'].agg(['min', 'max', 'mean'])
        yield length_stats
```
<img width="1600" height="757" alt="Screenshot 2025-07-10 at 9 38 24 PM" src="/images/github-assets/d246f929-e6b9-4828-afc2-4ae3eae92847.png" />


### Step 4: Annotate some operators as ‘View Result’ to display the same results as Notebook
Jupyter displays the output of every cell, whereas Texera shows only sink-operator outputs by default.

To view intermediate results, for example, the results after SortOperator, right-click the operator, select "View Result" shown in the drop-down menu, and re-run the workflow:

![2025-07-10 16 20 50](/images/github-assets/e2238f32-8b12-4e2a-ae5d-8eb8506edbee.png)

Texera will now show the operator’s output in the result panel. 
<img width="1600" height="767" alt="Screenshot 2025-07-10 at 9 41 25 PM" src="/images/github-assets/9e600e5f-f2a9-41dc-a8a5-9f261d45ac43.png" />


## 3. Tips

- **Utilize Texera native operators as much as possible**

Texera contains more than 110 built-in operators that cover data loading, cleaning, wrangling, visualization, and AI/ML. Replacing custom code with native operators makes workflows clearer and usually improves performance.


- **Identify the data dependencies in the Python code in order to connect operators**

In Texera, data flows along links. Before wiring operators, review the notebook to understand which variables feed which; then reproduce those dependencies via links so the executions matches the original notebook.















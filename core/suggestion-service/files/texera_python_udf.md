## What is Python UDF
User-defined Functions (UDFs) provide a means to incorporate custom logic into Texera. Texera offers comprehensive Python UDF APIs, enabling users to accomplish various tasks. This guide will delve into the usage of UDFs, breaking down the process step by step.


***


## UDF UI and Editor


The UDF operator offers the following interface, requiring the user to provide the following inputs: `Python code`, `worker count`, and `output schema`.

<p align="center">
<img width="450" alt="Screenshot 2023-07-04 at 12 51 37" src="https://github.com/Texera/texera/assets/17627829/ef57d6e5-9dbf-465f-bb57-3b48b8c33f32">
</p>

- <img width="150" align="left" alt="Screenshot 2023-07-04 at 13 25 59" src="https://github.com/Texera/texera/assets/17627829/e1404732-cdca-4fa2-a3ec-c0335b99a32e"> Users can click on the "Edit code content" button to open the UDF code editor, where they can enter their custom Python code to define the desired operator.

- <img width="150" align="left" alt="Screenshot 2023-07-04 at 13 27 22" src="https://github.com/Texera/texera/assets/17627829/0b7090ee-b931-4c13-80c3-85fadb12fa26"> Users have the flexibility to adjust the parallelism of the UDF operator by modifying the number of workers. The engine will then create the corresponding number of workers to execute the same operator in parallel.

- <img width="150" align="left" alt="Screenshot 2023-07-04 at 13 27 29" src="https://github.com/Texera/texera/assets/17627829/b2acc2a8-3e36-4f0b-b4d8-793117ebaf2f"> Users need to provide the output schema of the UDF operator, which describes the output data's fields.
  - The option `Retain input columns` allows users to include the input schema as the foundation for the output schema.
  - The `Extra output column(s)` list allows users to define additional fields that should be included in the output schema.

<br>
<br>
<br>

- <img width="150" align="left" alt="Screenshot 2023-07-04 at 13 04 31" src="https://github.com/Texera/texera/assets/17627829/1294c45b-5e21-4b8d-9b45-7f3e3886ecd7"> _Optionally_, users can click on the pencil icon located next to the operator name to make modifications to the name of the operator.


***

## Operator Definition

### Iterator-based operator
In Texera, all operators are implemented as iterators, including Python UDFs.
Concepturally, a defined operator is executed as:

```python
operator = UDF() # initialize a UDF operator

... # some other initialization logic

# the main process loop
while input_stream.has_more():
    input_data = next_data()
    output_iterator = operator.process(input_data)
    for output_data in output_iterator:
        send(output_data)

... # some cleanup logic

```

### Operator Life Cycle
The complete life cycle of a UDF operator consists of the following APIs:
1. `open() -> None` Open a context of the operator. Usually it can be used for loading/initiating some resources, such as a file, a model, or an API client. It will be invoked once per operator.
2. `process(data, port: int) -> Iterator[Optional[data]]` Process an input data from the given port, returning an iterator of optional data as output. It will be invoked once for every unit of data.
3. `on_finish(port: int) -> Iterator[Optional[data]]` Callback when one input port is exhausted, returning an iterator of optional data as output. It will be invoked once per port.
4. `close() -> None` Close the context of the operator. It will be invoked once per operator.


### Process Data APIs
There are three APIs to process the data in different units.

1. Tuple API.

```python

class ProcessTupleOperator(UDFOperatorV2):

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_

```
Tuple API takes one input tuple from a port at a time. It returns an iterator of optional `TupleLike` instances. A `TupleLike` is any data structure that supports key-value pairs, such as `pytexera.Tuple`, `dict`, `defaultdict`, `NamedTuple`, etc.

Tuple API is useful for implementing functional operations which are applied to tuples one by one, such as map, reduce, and filter.

2. Table API.
```python

class ProcessTableOperator(UDFTableOperator):

    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield table
```
Table API consumes a `Table` at a time, which consists of all the tuples from a port. It returns an iterator of optional `TableLike` instances. A `TableLike ` is a collection of `TupleLike`, and currently, we support `pytexera.Table` and `pandas.DataFrame` as a `TableLike` instance. More flexible types will be supported down the road. 

Table API is useful for implementing blocking operations that will consume all the data from one port, such as join, sort, and machine learning training.

3. Batch API.
```python

class ProcessBatchOperator(UDFBatchOperator):

    BATCH_SIZE = 10

    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        yield batch
```
Batch API consumes a batch of tuples at a time. Similar to `Table`, a `Batch` is also a collection of `Tuple`s; however, its size is defined by the `BATCH_SIZE`, and one port can have multiple batches. It returns an iterator of optional `BatchLike` instances. A `BatchLike ` is a collection of `TupleLike`, and currently, we support `pytexera.Batch` and `pandas.DataFrame` as a `BatchLike` instance. More flexible types will be supported down the road. 

The Batch API serves as a hybrid API combining the features of both the Tuple and Table APIs. It is particularly valuable for striking a balance between time and space considerations, offering a trade-off that optimizes efficiency.

_All three APIs can return an empty iterator by `yield None`._

### Schemas

A UDF has an input Schema and an output Schema. The input schema is determined by the upstream operator's output schema and the engine will make sure the input data (tuple, table, or batch) matches the input schema. On the other hand, users are required to define the output schema of the UDF, and it is the user's responsibility to make sure the data output from the UDF matches the defined output schema.

### Ports

- Input ports:
A UDF can take zero, one or multiple input ports, different ports can have different input schemas. Each port can take in multiple links, as long as they share the same schema.

- Output ports:
Currently, a UDF can only have exactly one output port. This means it cannot be used as a terminal operator (i.e., operator without output ports), or have more than one output port.

#### 1-out UDF

This UDF has zero input port and one output port. It is considered as a source operator (operator that produces data without an upstream). It has a special API:
```python

class GenerateOperator(UDFSourceOperator):

    @overrides
    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        yield 
```

This `produce()` API returns an iterator of `TupleLike`, `TableLike`, or simply `None`. 

See [Generator Operator](https://github.com/Texera/texera/blob/master/core/amber/src/main/python/pytexera/udf/examples/generator_operator.py) for an example of 1-out UDF.


#### 2-in UDF

This UDF has two input ports, namely `model` port and `tuples` port. The `tuples` port depends on the `model` port, which means that during the execution, the `model` port will execute first, and the `tuples` port will start after the `model` port consumes all its input data.
This dependency is particularly useful to implement machine learning inference operators, where a machine learning model is sent into the 2-in UDF through the `model` port, and becomes an operator state, then the tuples are coming in through the `tuples` port to be processed by the model.

An example of 2-in UDF:
```
class SVMClassifier(UDFOperatorV2):


    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:

        if port == 0: # models port
           self.model = tuple_['model']
        
        else: # tuples port
           tuple_['pred'] = self.model.predict(tuple_['text'])
           yield tuple_
```

_Currently, in 2-in UDF, "Retain input columns" will retain only the `tuples` port's input schema._
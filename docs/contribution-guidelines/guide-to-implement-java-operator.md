---
title: "Guide to Implement a Java Native Operator"
weight: 40
---


In this page, we'll explain the basic concepts in Texera and use examples to show how to implement an operator.

### Code structure of every operator:

Every operator ideally has three classes that are found in each operator package in `core\workflow-operator\src\main\scala\edu\uci\ics\amber\operator`
* LogicalOp
* OperatorExecutor
* OperatorExecutorConfig

### Basic concepts:

A Texera user constructs a workflow using the frontend, which consists of many operators. Each operator take input data from its previous operator(s), does some computation, and outputs the results to the next operator(s). 

Suppose we have the following sample records, each of which has an ID and a tweet.
```
id		tweet
1		"today is a good day"
2		"weather is bad during the day"
```

Each row is called a `Tuple`, and each column is called a `Field`.

```scala
// get the value of a field by column name
tuple1.getField("id") // result: 1
tuple1.getField("tweet") // result: "today is a good day"

// get the value by column index
tuple1.get(0) // result: 1
```

In this dataset, we have 2 columns, namely `id` of the integer type and `tweet` of the string type. This information is called a `Schema`.
A `schema` contains a list of `attributes`, and each `attribute` has a `name` (name of the column) and a `type` (data type of the column).

```scala
schema = tuple.getSchema()
schema.getAttributes().get(0) // Attribute("id", AttributeType.Integer)
schema.getAttributes().get(1) // Attribute("tweet", AttributeType.String)
```


### Example 1: Regular Expression (regex) operator

A regular expression operator matches a regular expression (regex) on each input tuple. For example, if we search the regex "weather" on the `tweet` attribute, then only tuple 2 will be the result. In other words, the regular expression operator is a kind of `filter()` operation in many programming languages.

To implement a regular expression operator, you will first need to write an `LogicalOp`. The following code is part of class [`RegexOpDesc`](https://github.com/apache/texera/blob/main/core/workflow-operator/src/main/scala/edu/uci/ics/amber/operator/regex/RegexOpDesc.scala) .

```scala
class RegexOpDesc extends FilterOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attribute")
  @JsonPropertyDescription("column to search regex on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("regex")
  @JsonPropertyDescription("regular expression")
  var regex: String = _

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("Case Insensitive")
  @JsonPropertyDescription("regex match is case sensitive")
  var caseInsensitive: Boolean = _
}
```

The regular expression operator needs to take 3 properties from the user, namely `attribute` (the name of the column to search on), `regex` (the regular expression itself) and `caseInsensitive` (whether case sensitive for this regular expression). 

The `@JsonProperty` annotation will let the system know that this property needs to come from the user input, and it will automatically generate the corresponding input form in the frontend. 
Inside `@JsonProperty`, `required = true` tells the frontend that this property is required from the user. The property also needs to provide a user-friendly title (inside `@JsonSchemaTitle` annotation) and a detailed description (inside `@JsonPropertyDescription` annotation). `@AutofillAttributeName` annotation tells the frontend to provide autocomplete on attribute name (name of the column).

This operator descriptor also needs to provide information about this operator, including a user-friendly name, description, the group it belongs to, and number of input/output ports.
```scala
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Regular Expression",
      operatorDescription = "Search a regular expression in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      numInputPorts = 1,
      numOutputPorts = 1
    )
```

Finally, the operator descriptor needs to specify its corresponding operator executor. An `OperatorExecutor`, or `OpExec` for short, contains the implementation of the processing logic in the operator. For the regular expression operator, it corresponds to `RegexOpExec`. The OpDesc supplies an `OpExecInitInfo` with a function that creates the corresponding operator executor `() => new RegexOpExec(this)`. When creating a PhysicalOp (e.g., using `oneToOnePhysicalOp` in this case, which is one type of physical operator that should be used in most cases), the `OpExecInitInfo` is passed in for the PhysicalOp to use. 

```scala
  PhysicalOp.oneToOnePhysicalOp(
      executionId,
      operatorIdentifier,
      OpExecInitInfo(_ => new RegexOpExec(this))
    )
```

The implementation of the regular expression operator executor is rather simple. Since this operator is doing a kind of `filter()` operation, it extends a pre-defined class `FilterOpExec`. It calls `setFilterFunc` to specify the filter function used by this operator: the `matchRegex` function. In `matchRegex`, we first get the string value of a column, and then test if the value matches the regex.

```scala
class RegexOpExec(val opDesc: RegexOpDesc) extends FilterOpExec {
  val pattern: Pattern = Pattern.compile(opDesc.regex)
  this.setFilterFunc(this.matchRegex)

  def matchRegex(tuple: Tuple): Boolean = {
    val tupleValue = tuple.getField(opDesc.attribute).toString
    return pattern.matcher(tupleValue).find
  }
}
```

This operator needs to be registered to let the system know its existence. In the `LogicalOp` class, we need to add a new entry, which specifies its operator descriptor class and a unique operator name.

```scala
@JsonSubTypes(
  Array(
    new Type(value = classOf[RegexOpDesc], name = "Regex"),
  )
)
abstract class LogicalOp extends PortDescriptor with Serializable {
}
```

Now this operator will be automatically available in the frontend. We can now start the system and test this operator.

To add an image for this operator, go to `core/gui/src/assets/operator_images`, then add an image with the _**SAME NAME**_ as what's specified in the operator registration. The image file should be in `png` format, with a transparent background, black and white, and should be square. 

For example, for the regex operator, the code `new Type(value = classOf[RegexOpDesc], name = "Regex")` specified a name `Regex`, then the image file name should be `Regex.png`. 


Summary: we have gone through the steps to implement a simple regular expression operator. This operator is a type of `filter()` operation. So it's built on top of a set of pre-defined classes, `FilterOpDesc`, `FilterOpExec`, and `FilterOpExecConfig`. 

### Example 2: Sentiment Analysis operator

A `map()` operation processes one input tuple and produces exactly one output tuple.  Next, we'll briefly explain the `map()` type of operators using the Sentiment Analysis operator as an example.

The sentiment analysis operator uses the Stanford NLP package to analyze the sentiment of a text.  Given the example dataset above, the output of this operator looks like this:
```
id		tweet					sentiment
1		"today is a good day"			"positive"
2		"weather is bad during the day"		"negative"
```


The following code is the implementation of class [`SentimentAnalysisOpDesc`](https://github.com/apache/texera/blob/main/core/workflow-operator/src/main/scala/edu/uci/ics/amber/operator/huggingFace/HuggingFaceSentimentAnalysisOpDesc.scala) in Java.

```java
public class SentimentAnalysisOpDesc extends MapOpDesc {

    @JsonProperty(required = true)
    @JsonSchemaTitle("attribute")
    @JsonPropertyDescription("column to perform sentiment analysis on")
    @AutofillAttributeName
    public String attribute;

    @JsonProperty(value = "result attribute", required = true, defaultValue = "sentiment")
    @JsonPropertyDescription("column name of the sentiment analysis result")
    public String resultAttribute;

    @Override
    public OneToOneOpExecConfig operatorExecutor() {
        return new OneToOneOpExecConfig(operatorIdentifier(), () -> new SentimentAnalysisOpExec(this));
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "Sentiment Analysis",
                "analysis the sentiment of a text using machine learning",
                OperatorGroupConstants.ANALYTICS_GROUP(),
                1, 1
        );
    }

    @Override
    public Schema getOutputSchema(Schema[] schemas) {
        if (resultAttribute == null || resultAttribute.trim().isEmpty()) {
            return null;
        }
        return Schema.newBuilder().add(schemas[0]).add(resultAttribute, AttributeType.STRING).build();
    }
}
```

You'll notice that this operator implements a new function, `getOutputSchema`. This is because this operator adds a new column called `sentiment`. The function `getOutputSchema` returns the output schema produced by this operator given an input schema. 

In this implementation, `resultAttribute` is the new column name given by the user (default value is "sentiment"). If the value is empty, we return a null value to indicate that the output schema cannot be produced. The result schema includes all the attributes from the input schema, plus a new attribute of  type string.

The regular expression operator does not implement this function because a `filter()` operation does not add or remove any columns. 

The implementation of `SentimentAnalysisOpExec` extends `MapOpExec` and provides a map function. You can check the implementation in the codebase.

### Generic operations

In Texera, currently we have 4 pre-defined operations you can extend.
  - `filter()`: filters out any input tuple if it doesn't satisfy a condition.
  - `map()`: for each input tuple, transforms it to exactly one output tuple. 
  - `flatmap()`: for each input tuple, transforms it to a list of output tuples. 
  - `aggregate()`: performs an aggregation, such as sum, count, average, etc. 

To implement an operator, you can first check if your operator can be implemented using the 4 pre-defined operations. You can find these pre-defined operations under [`texera/workflow/common/operators`](https://github.com/Texera/texera/tree/master/core/amber/src/main/scala/edu/uci/ics/texera/workflow/common/operators). Your own operator implementation should be in [`texera/workflow/operators/youroperator`](https://github.com/Texera/texera/tree/master/core/amber/src/main/scala/edu/uci/ics/texera/workflow/operators).

### Low-level OperatorExecutor API
For more complicated operators, if they cannot be implemented using these operations, then you need to implement `OperatorExecutor` using the following low-level interface.

```scala
trait IOperatorExecutor {

  def open(): Unit

  def close(): Unit

  def processTuple(tuple: Either[ITuple, InputExhausted], input: Int): Iterator[ITuple]

}
``` 

The `open()` and `close()` functions allow you to initialize and dispose any resources (such as opened files), respectively. They will be called once before and after the whole execution by the engine. The important function is `processTuple`, which implements the processing logic inside the operator. 

The `processTuple` function takes two parameters: `tuple` and `input`. Since an operator can have multiple input ports, and each input port can have multiple input operators connected to (e.g., Union), `input: Int` indicates which input port the current tuple is coming from. The parameter `tuple` is either a `Tuple` type or an `InputExhausted` type, indicating all data from an input operator has been exhausted. It returns an `Iterator[Tuple]`, which means zero or more output tuples can be produced following this input. `processTuple` will be called whenever a new input tuple arrives, and called once if the input is exhausted. When an input port is connected to multiple input operators, this `InputExhausted` will be processed multiple times (once per input operator).

## General content:
### User input information
Texera's backend is responsible for determining the UI information to the frontend. After receiving the information, the frontend efficiently translates and presents the content.
* Input Box
    
    ![image9](/images/github-assets/a013e8be-f9e1-45ec-a6e4-9a41d66b0287.png)

    Here is an example of a user input box, with the name “Client Id” and its description.
     ```python
    @JsonProperty(required=true)
    @JsonSchemaTitle("Client Id")
    @JsonPropertyDescription("Client id that uses to access Reddit API")
    var clientId: String = _
    ```
    

* Multiple selection
    
    ![image15](/images/github-assets/82fb2706-9445-47f0-92f9-0d93eb6d9e34.png)

    Here is an example of a multiple selection in the aggregate operator. 
    ```python
    @JsonProperty(value = "attribute", required = true)
    @JsonPropertyDescription("column to calculate average value")
    @AutofillAttributeName
    var attribute: String = _
    ```
    In the backend, we assign the attribute name list to fill the selections. Since it is multiselection, the type needs to be a list.
* Checkbox
    
    ![image4](/images/github-assets/bca18096-05e6-4696-992f-1a27349ca7f8.png)

    For the checkbox, we assign the data type to boolean. Here is an example in pythonUDF operator. By setting the data type to boolean, we successfully implement it as a checkbox.
	```python
    @JsonProperty(required = true, defaultValue = "true")
    @JsonSchemaTitle("Retain input columns")
    @JsonPropertyDescription("Keep the original input columns?")
    var retainInputColumns: Boolean = Boolean.box(false)
    ```

* List
    
    ![image10](/images/github-assets/dae7108d-6d2e-46f1-932e-939ab561f353.png)

    In pythonUDF operator, there is an example of a list, which is for the output schema. By clicking the blue button, we can add one more pair of attribute information. And the red button will delete such attribute information. In the backend, we have a list to hold the attribute values.
    ```python
    @JsonProperty
  @JsonSchemaTitle("Extra output column(s)")
  @JsonPropertyDescription(
    "Name of the newly added output columns that the UDF will produce, if any"
  )
  var outputColumns: List[Attribute] = List()
    ```

### Registration and icon
In the file `amber/src/main/scala/edu/uci/ics/texera/workflow/common/operators/LogicalOp.scala`, you will find a list of all registered operators, complete with their descriptor classes and names. After adding an operator's information, you can assign an icon to it. All operator icons are stored in the `/core/new-gui/src/assets/operator_images` directory. It's essential to ensure that the icon filename matches its respective operator descriptor name.



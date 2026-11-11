<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

---
title: "Java UDF"
description: "User-defined function operator in Java script"
category: "Java"
operator_type: "JavaUDF"
tags: [user-defined-functions, java]
---

[Home](../../../) > [User Defined Functions](../../) > [Java](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Java UDF script | ✓ | Code (java) | `See template below` | Input your code here |
| Worker count | ✓ | Integer | 1 | Specify how many parallel workers to launch |
| Retain input columns | ✓ | Boolean | true | Keep the original input columns? |
| Extra output column(s) |  | List<Attribute> | - | Name of the newly added output columns that the<br>UDF will produce, if any |
| ↳ Attribute Name | ✓ | String | - |  |
| ↳ Attribute Type | ✓ | string, integer, long, double, boolean,<br>timestamp, binary, large_binary | - |  |

#### Default Code Template

**Java UDF script**

```java
import org.apache.texera.amber.operator.map.MapOpExec;
import org.apache.texera.amber.core.tuple.Tuple;
import org.apache.texera.amber.core.tuple.TupleLike;
import scala.Function1;
import java.io.Serializable;

public class JavaUDFOpExec extends MapOpExec {
    public JavaUDFOpExec () {
        this.setMapFunc((Function1<Tuple, TupleLike> & Serializable) this::processTuple);
    }
    
    public TupleLike processTuple(Tuple tuple) {
        return tuple;
    }
}
```

### UI parameters

Use **Parameters → Add parameter** in the Java UDF properties panel, or annotate an instance field inside `JavaUDFOpExec`:

```java
import org.apache.texera.amber.operator.udf.java.UiParameter;

// Inside JavaUDFOpExec:
@UiParameter("count") private final int count;
@UiParameter("label") private String label;
@UiParameter(value="model", resource="model") private String modelDirectory;
```

The panel infers names and types from the declarations and lets users edit values. Model and dataset parameters use the existing version selector; the field receives the selected version's mounted directory when the workflow runs. Use `resource="dataset"` for a dataset.

| Java field type | Parameter type |
| --- | --- |
| `String` | string |
| `int` / `Integer` | integer |
| `long` / `Long` | long |
| `double` / `Double` | double |
| `boolean` / `Boolean` | boolean |
| `java.sql.Timestamp` | timestamp |

Declare one field per annotation, without a field initializer. Static fields and arrays are unsupported. Values are validated and compiled into instance field initializers, so they are available in the constructor, `open()`, and tuple processing. Resource parameters must be strings. Timestamp text uses ISO dates or datetimes, with UTC assumed when no offset is given. Long values retain all 64 bits.

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |

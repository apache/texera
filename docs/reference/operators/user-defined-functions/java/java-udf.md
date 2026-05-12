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

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |

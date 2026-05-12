---
title: "1-out R UDF"
description: "User-defined function operator in R script"
category: "R"
operator_type: "RUDFSource"
tags: [user-defined-functions, r]
---

[Home](../../../) > [User Defined Functions](../../) > [R](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| R Source UDF Script | ✓ | Code (r) | `See template below` | Input your code here |
| Worker count | ✓ | Integer | 1 | Specify how many parallel workers to launch |
| Use Tuple API? | ✓ | Boolean | false | Check this box to use Tuple API, leave unchecked<br>to use Table API |
| Columns |  | List<Attribute> | - | The columns of the source |
| ↳ Attribute Name | ✓ | String | - |  |
| ↳ Attribute Type | ✓ | string, integer, long, double, boolean,<br>timestamp, binary, large_binary | - |  |

#### Default Code Template

**R Source UDF Script**

```r
# If using Table API:
# function() { 
#   return (data.frame(Column_Here = "Value_Here")) 
# }

# If using Tuple API:
# library(coro)
# coro::generator(function() {
#   yield (list(text= "hello world!"))
# })
```

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |

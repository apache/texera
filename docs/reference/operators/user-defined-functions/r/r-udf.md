---
title: "R UDF"
description: "User-defined function operator in R script"
category: "R"
operator_type: "RUDF"
tags: [user-defined-functions, r]
---

[Home](../../../) > [User Defined Functions](../../) > [R](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| R UDF Script | ✓ | Code (r) | `See template below` | Input your code here |
| Worker count | ✓ | Integer | 1 | Specify how many parallel workers to launch |
| Use Tuple API? | ✓ | Boolean | false | Check this box to use Tuple API, leave unchecked<br>to use Table API |
| Retain input columns | ✓ | Boolean | true | Keep the original input columns? |
| Extra output column(s) |  | List<Attribute> | - | Name of the newly added output columns that the<br>UDF will produce, if any |
| ↳ Attribute Name | ✓ | String | - |  |
| ↳ Attribute Type | ✓ | string, integer, long, double, boolean,<br>timestamp, binary, large_binary | - |  |

#### Default Code Template

**R UDF Script**

```r
# If using Table API:
# function(table, port) { 
#   return (table) 
# }

# If using Tuple API:
# library(coro)
# coro::generator(function(tuple, port) {
#   yield (tuple)
# })
```

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |

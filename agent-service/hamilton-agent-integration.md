# Integrating Hamilton as the Dataflow Backend for Agent-Generated Pipelines

## 1. Motivation

Our agent framework uses a ReAct-style LLM agent to construct data analysis pipelines as dataflow DAGs. The agent generates Python functions that fall into two categories:

- **`load()`**: Source operators that produce a `pd.DataFrame` with no upstream dependencies (e.g., reading a CSV file).
- **`process(op1, op2, ...)`**: Transform operators that consume one or more upstream DataFrames and produce a new `pd.DataFrame`.

The agent assigns each operator an `operatorId`, and the service layer is responsible for wiring operators together (creating links based on parameter names), executing the workflow up to a target operator, and returning the resulting DataFrame along with metadata (shape, columns, sample rows).

We need an open-source workflow system that supports:

1. **DAG-based execution** (not just chains or trees — must handle fan-in/fan-out).
2. **Python UDFs** as first-class operators.
3. **API accessibility** to programmatically construct the workflow and execute it up to a specific operator.

After surveying the landscape (Airflow, Beam, Dagster, Prefect, Kedro, Dask, KNIME, Daggr, and others), **Apache Hamilton** emerged as the strongest semantic match.

## 2. Why Hamilton?

[Apache Hamilton](https://hamilton.dagworks.io/) is a lightweight Python framework for describing dataflows as regular Python functions. Its core design maps almost 1:1 onto our agent's `load`/`process` semantics:

| Our Agent Semantics | Hamilton Equivalent |
|---|---|
| `operatorId` | Function name |
| `load() -> DataFrame` | Function with **no parameters** returning `pd.DataFrame` |
| `process(op1, op2) -> DataFrame` | Function with **parameters matching other function names** |
| `addLink(source, target)` | **Automatic**: parameter names are the dependency references |
| `executeWorkflow(targetOperatorIds=["X"])` | `dr.execute(["X"])` — runs only the upstream subgraph |
| Returns DataFrame + metadata | Returns `{"X": <DataFrame>}` |

Hamilton's key properties that align with our requirements:

- **DAG, not chain**: Functions can depend on multiple upstream functions (fan-in), and a single function's output can feed multiple downstream functions (fan-out). The resulting structure is a true DAG.
- **Selective execution**: `dr.execute(["node_name"])` computes only the minimal upstream subgraph needed to produce that node. Unrelated branches are never executed.
- **Pure Python**: No YAML, no config files, no external scheduler. Operators are plain Python functions with type annotations.
- **Lightweight**: Hamilton is a library, not a platform. No server, no database, no Docker required. It runs anywhere Python runs.
- **DataFrame-native**: Hamilton was originally built for pandas DataFrame generation at Stitch Fix and defaults to returning DataFrames.

## 3. Hamilton Basics

### 3.1 Operators as Functions

In Hamilton, you define operators as Python functions in a module. The function name becomes the operator ID, and parameter names declare upstream dependencies:

```python
# operators.py
import pandas as pd

# LOAD operator: no parameters = source node
def customers() -> pd.DataFrame:
    return pd.read_csv("/data/customers.csv")

# PROCESS operator: parameter "customers" = upstream link
def adult_customers(customers: pd.DataFrame) -> pd.DataFrame:
    return customers[customers["age"] >= 18]
```

Hamilton inspects the function signatures and automatically builds the DAG:

```
customers  -->  adult_customers
```

### 3.2 Executing the DAG

A `Driver` builds the DAG from one or more Python modules and executes it:

```python
from hamilton import driver
import operators

dr = driver.Builder().with_modules(operators).build()

# Execute up to "adult_customers" — only runs customers() then adult_customers()
result = dr.execute(["adult_customers"])
df = result["adult_customers"]  # pd.DataFrame
```

### 3.3 Multi-Input Operators (Fan-In)

Functions can depend on multiple upstream operators, creating true DAG structures:

```python
def customer_spending(
    orders_with_totals: pd.DataFrame,
    adult_customers: pd.DataFrame,
) -> pd.DataFrame:
    return orders_with_totals.merge(adult_customers, on="customer_id")
```

This creates two incoming links — a fan-in that cannot be represented as a simple chain or tree.

## 4. Complete Example

### 4.1 Operator Definitions

```python
# operators.py
import pandas as pd

# ── LOAD operators (source nodes, no parameters) ──

def customers() -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 17, 30, 15, 22],
        "city": ["NYC", "LA", "NYC", "Chicago", "LA"],
    })

def products() -> pd.DataFrame:
    return pd.DataFrame({
        "product_id": [101, 102, 103],
        "product_name": ["Widget", "Gadget", "Doohickey"],
        "price": [9.99, 24.99, 4.99],
    })

def orders() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": [1001, 1002, 1003, 1004, 1005],
        "customer_id": [1, 3, 5, 1, 3],
        "product_id": [101, 103, 102, 102, 101],
        "quantity": [2, 1, 3, 1, 5],
    })

# ── PROCESS operators (parameters = upstream operator IDs) ──

def adult_customers(customers: pd.DataFrame) -> pd.DataFrame:
    return customers[customers["age"] >= 18]

def orders_with_products(
    orders: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    return orders.merge(products, on="product_id")

def orders_with_totals(orders_with_products: pd.DataFrame) -> pd.DataFrame:
    df = orders_with_products.copy()
    df["total_price"] = df["quantity"] * df["price"]
    return df

def customer_spending(
    orders_with_totals: pd.DataFrame,
    adult_customers: pd.DataFrame,
) -> pd.DataFrame:
    return orders_with_totals.merge(adult_customers, on="customer_id")
```

### 4.2 The Resulting DAG

Hamilton automatically builds this DAG from the function signatures:

```
    customers          products        orders
       |                  |               |
       v                  +-------+-------+
  adult_customers                 |
       |                          v
       |               orders_with_products
       |                          |
       |                          v
       |               orders_with_totals
       |                          |
       +------------+-------------+
                    |
                    v
            customer_spending
```

This is a true DAG with fan-out (e.g., `customers` feeds both `adult_customers` and potentially other branches) and fan-in (e.g., `customer_spending` consumes two independent branches).

### 4.3 Selective Execution

```python
from hamilton import driver
import operators

dr = driver.Builder().with_modules(operators).build()

# Only the LEFT branch executes (customers -> adult_customers)
result = dr.execute(["adult_customers"])

# Only the RIGHT branch executes (orders + products -> join -> totals)
result = dr.execute(["orders_with_totals"])

# BOTH branches execute and converge (full DAG)
result = dr.execute(["customer_spending"])

# Multiple targets: shared upstream nodes run only once
result = dr.execute(["adult_customers", "orders_with_totals"])
```

### 4.4 Extracting Metadata

After execution, the service layer can extract metadata from the result:

```python
result = dr.execute(["customer_spending"])
df = result["customer_spending"]

metadata = {
    "operatorId": "customer_spending",
    "shape": df.shape,              # (rows, cols)
    "columns": list(df.columns),
    "dtypes": {col: str(dt) for col, dt in df.dtypes.items()},
    "sample_rows": df.head(5).to_dict(orient="records"),
}
```

## 5. Architecture: Agent-Service-Hamilton Pipeline

### 5.1 Overview

The integration has three layers:

```
+------------------+       +---------------------+       +------------------+
|   LLM Agent      |       |   Service Layer     |       |   Hamilton       |
|                  |       |                     |       |                  |
|  Generates:      | ----> |  Translates:        | ----> |  Executes:       |
|  - operatorId    |       |  - Wraps code into  |       |  - Builds DAG    |
|  - load/process  |       |    typed functions   |       |  - Runs subgraph |
|    function body |       |  - Creates module   |       |  - Returns DF    |
|  - target to     |       |  - Calls dr.execute |       |                  |
|    execute       |       |  - Returns metadata |       |                  |
+------------------+       +---------------------+       +------------------+
```

### 5.2 What the Agent Generates

The agent produces a tool call like this:

```json
{
  "operatorId": "filtered_customers",
  "code": "def process(customers) -> pd.DataFrame:\n    return customers[customers['age'] > 18]"
}
```

Or for a source:

```json
{
  "operatorId": "customers",
  "code": "def load() -> pd.DataFrame:\n    return pd.read_csv('/data/customers.csv')"
}
```

### 5.3 What the Service Layer Does

The service layer translates the agent's `load`/`process` convention into Hamilton-compatible functions:

```python
import types
import pandas as pd
import textwrap
from hamilton import driver


class HamiltonAgentService:
    """Translates agent-generated load/process code into Hamilton operators."""

    def __init__(self):
        self.operators = {}  # operatorId -> function

    def add_operator(self, operator_id: str, code: str):
        """
        Parse agent-generated code and register as a Hamilton-compatible function.

        Agent generates either:
          def load() -> pd.DataFrame: ...
          def process(op1, op2) -> pd.DataFrame: ...

        Service layer renames the function to operator_id so Hamilton
        uses it as the node name, and the parameter names become the
        upstream dependency references.
        """
        # Execute the code to get the function object
        local_ns = {"pd": pd}
        exec(code, local_ns)

        if "load" in local_ns:
            func = local_ns["load"]
        elif "process" in local_ns:
            func = local_ns["process"]
        else:
            raise ValueError("Code must define either load() or process()")

        # Rename the function to the operator ID
        # Hamilton uses function.__name__ as the node name
        func.__name__ = operator_id
        func.__qualname__ = operator_id

        self.operators[operator_id] = func

    def _build_module(self) -> types.ModuleType:
        """Package all registered operators into a Python module."""
        mod = types.ModuleType("agent_pipeline")
        for op_id, func in self.operators.items():
            setattr(mod, op_id, func)
        return mod

    def execute(self, target_operator_id: str) -> dict:
        """
        Build the DAG and execute up to the target operator.
        Returns the DataFrame and metadata.
        """
        mod = self._build_module()
        dr = driver.Builder().with_modules(mod).build()

        result = dr.execute([target_operator_id])
        df = result[target_operator_id]

        return {
            "operatorId": target_operator_id,
            "success": True,
            "shape": {"rows": df.shape[0], "cols": df.shape[1]},
            "columns": [
                {"name": col, "dtype": str(dtype)}
                for col, dtype in df.dtypes.items()
            ],
            "records": df.head(10).to_dict(orient="records"),
        }
```

### 5.4 End-to-End Walkthrough

Here is a multi-turn agent interaction translated through the service layer:

```python
service = HamiltonAgentService()

# ── Agent Turn 1: Load customers ──
service.add_operator("customers", textwrap.dedent("""
    def load() -> pd.DataFrame:
        return pd.DataFrame({
            "customer_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 17, 30, 15, 22],
        })
"""))

result = service.execute("customers")
# {
#   "operatorId": "customers",
#   "shape": {"rows": 5, "cols": 3},
#   "columns": [{"name": "customer_id", "dtype": "int64"}, ...],
#   "records": [{"customer_id": 1, "name": "Alice", "age": 25}, ...]
# }

# ── Agent Turn 2: Filter to adults ──
service.add_operator("adults", textwrap.dedent("""
    def process(customers: pd.DataFrame) -> pd.DataFrame:
        return customers[customers["age"] >= 18]
"""))

result = service.execute("adults")
# Hamilton runs: customers() -> adults()
# shape: {"rows": 3, "cols": 3}

# ── Agent Turn 3: Load products + join ──
service.add_operator("products", textwrap.dedent("""
    def load() -> pd.DataFrame:
        return pd.DataFrame({
            "product_id": [101, 102],
            "price": [9.99, 24.99],
        })
"""))

service.add_operator("adult_products", textwrap.dedent("""
    def process(adults: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
        adults_cp = adults.copy()
        adults_cp["_key"] = 1
        products_cp = products.copy()
        products_cp["_key"] = 1
        merged = adults_cp.merge(products_cp, on="_key").drop("_key", axis=1)
        return merged
"""))

result = service.execute("adult_products")
# Hamilton runs the full upstream subgraph:
#   customers() -> adults()  --+
#                               +--> adult_products()
#   products()              --+
# shape: {"rows": 6, "cols": 5}

# ── Can still execute earlier operators ──
result = service.execute("customers")
# Only runs customers(), nothing else
```

### 5.5 The Translation Rules

The service layer applies a simple translation from the agent's convention to Hamilton's convention:

| Agent Convention | Translation | Hamilton Convention |
|---|---|---|
| `operatorId = "X"` | Rename function to `X` | `def X(...) -> pd.DataFrame` |
| `def load()` | No-parameter function | Source node (no upstream links) |
| `def process(a, b)` | Parameters named after other operator IDs | Links auto-created from param names |
| `executeWorkflow(target=["X"])` | Call `dr.execute(["X"])` | Executes minimal upstream subgraph |

The translation is mechanical and requires no semantic understanding. The service layer's job is to:

1. Extract the function from the agent's code string via `exec()`.
2. Rename `__name__` to the `operatorId`.
3. Register it in the operators dict.
4. Rebuild the Hamilton driver and call `execute()`.

## 6. Key Architectural Considerations

### 6.1 DAG Rebuilding

Hamilton builds the DAG at `driver.Builder().build()` time by inspecting all functions in the provided modules. It does not support adding nodes incrementally to an existing DAG. Each time the agent adds a new operator, the service layer must rebuild the driver.

In practice, this is fast because it only involves Python function introspection (no compilation, no network calls). For a DAG with hundreds of nodes, the rebuild is sub-second.

### 6.2 Caching and Re-execution

Hamilton supports caching via `driver.Builder().with_cache().build()`. This means that if the agent adds a new downstream operator and executes it, Hamilton can reuse cached results from previously executed upstream operators rather than re-computing them. This is analogous to the "execute only what changed" behavior in the Texera agent framework.

### 6.3 Error Isolation

If a `process` function raises an exception, Hamilton propagates the error with the node name and traceback. The service layer can catch this and return structured error information to the agent, enabling the ReAct loop to retry or modify the operator.

### 6.4 Comparison to Texera

| Aspect | Texera Agent Framework | Hamilton-Based Approach |
|---|---|---|
| DAG construction | Incremental `addOperator` / `addLink` API calls | Rebuild from function dict on each turn |
| Operator types | Rich built-in operators (CSV scan, aggregate, join, chart) | Only Python UDFs (load/process) |
| Execution model | Streaming, distributed | Single-process, in-memory |
| Partial execution | `targetOperatorIds` parameter | `dr.execute(["target"])` |
| GUI | Full visual DAG editor | None (code-only) |
| State persistence | Workflow DAG saved as JSON | Functions in memory (or serialized) |
| Scalability | Distributed across workers | Single machine (Dask/Ray adapters available) |

## 7. Other Eligible Systems

While Hamilton is the strongest match, three other systems also meet the core requirements (programmatic construction + linking + execute-to-operator):

### 7.1 Dagster

Dagster supports partial execution via `op_selection=["*op_name"]` which runs an op and all its ancestors. However, wiring between ops must be declared explicitly in a `@job` function rather than inferred from parameter names. The service layer would need to generate both the `@op` definitions and the `@job` wiring code.

### 7.2 Kedro

Kedro supports node selection via `node_names=["X"]` and allows programmatic pipeline construction via `Pipeline([node(...)])`. Wiring is explicit via `inputs`/`outputs` string arguments on each node. Like Dagster, the service layer would need to generate explicit wiring declarations.

### 7.3 Dask

Dask's `delayed` API allows any Python function to become a lazy computation node. Calling `.compute()` on any node executes only its upstream dependencies. However, Dask provides no operator registry, no DAG introspection, and no execution metadata. The service layer would need to build most of the workflow management infrastructure from scratch.

## 8. Summary

Hamilton provides the most natural backend for translating agent-generated `load`/`process` operators into executable dataflow DAGs because:

1. Its function-name-as-node-ID convention matches the agent's `operatorId` concept.
2. Its parameter-name-as-dependency-reference convention matches how the agent's `process` functions declare inputs.
3. Its `dr.execute(["target"])` API provides exact execute-to-operator semantics with minimal upstream subgraph computation.
4. It returns pandas DataFrames natively, matching the agent's expected output format.
5. It requires zero infrastructure — the entire system runs in-process as a Python library.

The service layer's translation from agent conventions to Hamilton is mechanical: rename the function, register it, rebuild the driver, execute, and return metadata.

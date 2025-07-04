## Workflow Suggestion Generator

### Prompt Generation Layer

This layer is responsible for processing the given information regarding the workflow, compilation state, execution state and result, into the better natural language description. Once these natural language descriptions are generated, they can be passed to the LLM agent.

Prompt generation can be divided into 2 major packages, each package provides a single endpoint:
#### 1. Workflow Static Interpretation
- Endpoint name: interpretWorkflow
- Endpoint intput parameters:
    - workflow (dict) 
    - input schema for each operator (dict)
    - static error for each operator (dict)
    - interpretation method type
        1. RAW: simply use the below template
            ```
            Here is the workflow dict: 
            ${string representation of workflow dict}

            Here is the input schema for each operator:
            ${string representation of the input schema dict}
            ```
        2. BY_PATH
            - use the `TexeraWorkflow` class to parse the workflow dict and input schema dict
            - parse out all the paths from the DAG structure. A path is a sub workflow which is a single line, from source operator to the sink operator. This subworkflow should also carry the schema information extracted from the input schema dict, and the static error information, on each operator

            - use the below template to generate the description from the paths and schemas:
            ```
            Here are the existing paths in this workflow and related schemas

            Path1: ${string representation of the path's sub-workflow}

            Path2: ${string representation of the path's sub-workflow}
            ...
            ```
- Endpoint output parameters:
    - natural language description (a string)



#### 2. Workflow Interpretation with Execution information
TODO

### LLM Agent Layer

This layer is responsible for calling language models to get the response (the suggestions).

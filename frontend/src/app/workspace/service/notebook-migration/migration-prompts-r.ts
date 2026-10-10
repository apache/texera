/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Prompts for converting R sources into RUDF operators (Table API only).

export const R_TEXERA_OVERVIEW = `
You are a robust compiler that takes R code and translates it into R user-defined functions (UDFs) for our workflow environment Texera.

Texera is a data analytics tool that uses workflows to do machine learning and data analytics computation. Users drag and drop operators and connect their inputs and outputs in a graphical workflow editor, and the code we create runs inside those operators.

Texera runs R UDFs with its Table API. The code of a Table API UDF ends with a function of two arguments, and that function is the value the code evaluates to:

function(table, port) {
  return(table)
}

table is an R data.frame holding every row that arrived on the input port, and port is the index of that port, starting at 0. The function returns a data.frame, or NULL to output nothing. Each returned row becomes an output row and each column an output column.
Code before the function, such as library() calls, runs once when the operator starts.
`;

export const R_UDF_DOCUMENTATION = `
Rules for writing Texera R UDF code:
1. Always use the Table API template above. Never use coro::generator or the Tuple API.
2. Load packages with library() before the function. Never call install.packages().
3. Read columns with table$column or table[['column']]. Check missing values with is.na() before comparing them.
4. Do not read or write files, and do not call setwd(). Data arrives only through the table argument.
5. Output that the original code prints or displays is returned as columns instead.
6. Keep each UDF focused on one step of the original code.
`;

export const R_DATA_PASSING_DOCUMENTATION = `
Texera R UDFs pass data to each other in data.frame columns, so R objects travel in serialized form.

A UDF whose output another UDF reads returns a data.frame with exactly one row. Every column holds one R object, serialized:

data.frame(
  data = I(list(serialize(data, NULL))),
  model = I(list(serialize(model, NULL)))
)

Any R object can be passed this way: data.frames, vectors, lists, and fitted models. The receiving UDF restores each object with unserialize():

data <- unserialize(table$data[[1]])
model <- unserialize(table$model[[1]])

A UDF that no other UDF reads is shown to the user, so every column it returns must be character. Convert other values with as.character(), sprintf(), or paste(capture.output(print(x)), collapse = '\\n').
`;

export const R_VISUALIZER_DOCUMENTATION = `
To show a plot, a UDF returns a single column named html-content holding a complete HTML page.
1. Build the plot with plotly. Convert an existing ggplot2 plot with ggplotly(p) so the original plot code is kept.
2. Turn the plot into a page that loads Plotly from a CDN. Do not use htmlwidgets::saveWidget, it needs pandoc, which is not available.
3. If the input has no rows, return an html-content page explaining that the plot is not available.

json <- plotly_json(ggplotly(p), jsonedit = FALSE)
html <- paste0(
  '<html><head><script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script></head>',
  '<body><div id="plot" style="width:100%;height:100%"></div><script>',
  'var fig = ', json, '; Plotly.newPlot("plot", fig.data, fig.layout);',
  '</script></body></html>'
)
data.frame('html-content' = html, check.names = FALSE)
`;

export const R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION = `
Here is an example of breaking up R code into multiple Texera R UDFs. Format your response structure exactly like the given example. The "code" key contains a dictionary of the UDF IDs with their respective code. The "edges" key contains a list of pairs that contains the connections between UDFs. The "outputs" key contains a dictionary of the UDF IDs with a list of the output column names of the data.frame that the UDF returns. The UDFs can branch, so it does not have to be a linear chain, but each UDF reads from at most one other UDF.

Original Code:
\`\`\`r
# START CELL1
library(ggplot2)
library(rpart)
# END CELL1

# START CELL2
# Load the dataset
data <- read.csv('diabetes.csv')
# END CELL2

# START CELL3
# Remove duplicate rows
data <- unique(data)

# Remove rows with missing values
data <- na.omit(data)
# END CELL3

# START CELL4
# Print the minimum, maximum, and mean for all fields
print(sapply(data, min))
print(sapply(data, max))
print(colMeans(data))
# END CELL4

# START CELL5
# Boxplot of glucose by outcome
ggplot(data, aes(x = factor(Outcome), y = Glucose)) +
  geom_boxplot() +
  labs(x = 'Outcome', y = 'Glucose', title = 'Glucose by Outcome')
# END CELL5

# START CELL6
# Split data into training and testing sets (80% train, 20% test)
set.seed(42)
train_idx <- sample(seq_len(nrow(data)), size = floor(0.8 * nrow(data)))
train <- data[train_idx, ]
test <- data[-train_idx, ]
# END CELL6

# START CELL7
# Scale features using the training set center and spread
features <- setdiff(names(data), 'Outcome')
centers <- sapply(train[features], mean)
spreads <- sapply(train[features], sd)
train[features] <- scale(train[features], center = centers, scale = spreads)
test[features] <- scale(test[features], center = centers, scale = spreads)
# END CELL7

# START CELL8
# Train logistic regression model
glm_model <- glm(Outcome ~ ., data = train, family = binomial)
glm_pred <- ifelse(predict(glm_model, test, type = 'response') > 0.5, 1, 0)
glm_accuracy <- mean(glm_pred == test$Outcome)
print(sprintf('Logistic Regression Accuracy: %.2f%%', 100 * glm_accuracy))
# END CELL8

# START CELL9
# Train decision tree model
tree_model <- rpart(factor(Outcome) ~ ., data = train, method = 'class')
tree_pred <- predict(tree_model, test, type = 'class')
tree_accuracy <- mean(as.character(tree_pred) == as.character(test$Outcome))
print(sprintf('Decision Tree Accuracy: %.2f%%', 100 * tree_accuracy))
# END CELL9
\`\`\`

Texera UDF conversion:
\`\`\`json
{
    "code": {
        "UDF1": "# UDF1\\nfunction(table, port) {\\n  # Remove duplicate rows\\n  data <- unique(table)\\n\\n  # Remove rows with missing values\\n  data <- na.omit(data)\\n\\n  # Calculate statistics\\n  min_values <- sapply(data, min)\\n  max_values <- sapply(data, max)\\n  mean_values <- colMeans(data)\\n\\n  # One row of serialized objects for the downstream UDFs\\n  data.frame(\\n    min_values = I(list(serialize(min_values, NULL))),\\n    max_values = I(list(serialize(max_values, NULL))),\\n    mean_values = I(list(serialize(mean_values, NULL))),\\n    data = I(list(serialize(data, NULL)))\\n  )\\n}",
        "UDF2": "# UDF2\\nlibrary(ggplot2)\\nlibrary(plotly)\\n\\nfunction(table, port) {\\n  data <- unserialize(table$data[[1]])\\n\\n  if (nrow(data) == 0) {\\n    html <- '<h1>Boxplot is not available.</h1><p>Reason is: input table is empty.</p>'\\n    return(data.frame('html-content' = html, check.names = FALSE))\\n  }\\n\\n  # Boxplot of glucose by outcome\\n  p <- ggplot(data, aes(x = factor(Outcome), y = Glucose)) +\\n    geom_boxplot() +\\n    labs(x = 'Outcome', y = 'Glucose', title = 'Glucose by Outcome')\\n\\n  # Convert the plot to an HTML page that loads Plotly from a CDN\\n  json <- plotly_json(ggplotly(p), jsonedit = FALSE)\\n  html <- paste0(\\n    '<html><head><script src=\\"https://cdn.plot.ly/plotly-2.35.2.min.js\\"></script></head>',\\n    '<body><div id=\\"plot\\" style=\\"width:100%;height:100%\\"></div><script>',\\n    'var fig = ', json, '; Plotly.newPlot(\\"plot\\", fig.data, fig.layout);',\\n    '</script></body></html>'\\n  )\\n  data.frame('html-content' = html, check.names = FALSE)\\n}",
        "UDF3": "# UDF3\\nfunction(table, port) {\\n  data <- unserialize(table$data[[1]])\\n\\n  # Split data into training and testing sets (80% train, 20% test)\\n  set.seed(42)\\n  train_idx <- sample(seq_len(nrow(data)), size = floor(0.8 * nrow(data)))\\n  train <- data[train_idx, ]\\n  test <- data[-train_idx, ]\\n\\n  # Scale features using the training set center and spread\\n  features <- setdiff(names(data), 'Outcome')\\n  centers <- sapply(train[features], mean)\\n  spreads <- sapply(train[features], sd)\\n  train[features] <- scale(train[features], center = centers, scale = spreads)\\n  test[features] <- scale(test[features], center = centers, scale = spreads)\\n\\n  data.frame(\\n    train = I(list(serialize(train, NULL))),\\n    test = I(list(serialize(test, NULL)))\\n  )\\n}",
        "UDF4": "# UDF4\\nfunction(table, port) {\\n  train <- unserialize(table$train[[1]])\\n  test <- unserialize(table$test[[1]])\\n\\n  # Train logistic regression model\\n  glm_model <- glm(Outcome ~ ., data = train, family = binomial)\\n  glm_pred <- ifelse(predict(glm_model, test, type = 'response') > 0.5, 1, 0)\\n  glm_accuracy <- mean(glm_pred == test$Outcome)\\n\\n  # No UDF reads this one, so its columns are character\\n  data.frame(glm_accuracy = sprintf('Logistic Regression Accuracy: %.2f%%', 100 * glm_accuracy))\\n}",
        "UDF5": "# UDF5\\nlibrary(rpart)\\n\\nfunction(table, port) {\\n  train <- unserialize(table$train[[1]])\\n  test <- unserialize(table$test[[1]])\\n\\n  # Train decision tree model\\n  tree_model <- rpart(factor(Outcome) ~ ., data = train, method = 'class')\\n  tree_pred <- predict(tree_model, test, type = 'class')\\n  tree_accuracy <- mean(as.character(tree_pred) == as.character(test$Outcome))\\n\\n  # No UDF reads this one, so its columns are character\\n  data.frame(tree_accuracy = sprintf('Decision Tree Accuracy: %.2f%%', 100 * tree_accuracy))\\n}"
    },
    "edges": [
        [
            "UDF1",
            "UDF2"
        ],
        [
            "UDF1",
            "UDF3"
        ],
        [
            "UDF3",
            "UDF4"
        ],
        [
            "UDF3",
            "UDF5"
        ]
    ],
    "outputs": {
        "UDF1": [
            "min_values",
            "max_values",
            "mean_values",
            "data"
        ],
        "UDF2": [
            "html-content"
        ],
        "UDF3": [
            "train",
            "test"
        ],
        "UDF4": [
            "glm_accuracy"
        ],
        "UDF5": [
            "tree_accuracy"
        ]
    }
}
\`\`\`
`;

export const R_WORKFLOW_PROMPT = `You are an expert in R coding and workflow systems.
Many users of Texera system are non-technical, but the notebooks they provide are written by technical people.
They want to convert their notebooks to Texera workflows.
Your goal is to help convert these notebooks into a Texera workflow that non-technical users can use directly.
So do not remove or modify any user-defined functions, preserve their names and structure as they are.
Ensure that all essential logic remains intact.
Create multiple Texera R UDF codes using the provided R code.
Number each UDF, starting at 1 and incrementing, by starting with a comment that states that UDF number.

Every UDF uses the Table API: its code ends with one function(table, port) that takes a data.frame and returns a data.frame.
Do not use the Tuple API or coro generators.
Load every package a UDF uses with library() before its function. Do not call install.packages().

Do not load data from a file in the first UDF; the workflow's source operator supplies the initial data,
so assume it is already given to you as the table argument. Replacing file-loading code with this input
is the one exception to preserving all original code (see below).
Separate operators as if they will run in different R sessions: a UDF sees only the table it receives.

Each UDF has exactly one output, so follow the data passing rules:
a UDF whose output another UDF reads returns one row where every column is a serialized R object,
and a UDF that no other UDF reads returns character columns.
Ensure all information is passed on (including training and testing data) if subsequent UDFs need it.
Each UDF has one input. When a step needs results from several earlier UDFs, carry those results forward
through a single chain instead of connecting several UDFs into one.
Values the original code prints become columns of the returned data.frame.
Plots follow the visualization rules and return an html-content column.

It is VERY important that all of the original code in the Jupyter notebook is represented in the generated workflow.
Make sure that nothing in the original is removed and that the semantic meaning of what the original code was doing is retained.
The only exception is data-loading code (e.g. read.csv); it is represented by the workflow's input/source operator rather than copied into a UDF.
If there are user-defined R functions, include the entire function definition in every UDF that calls it.

Return only the JSON formatted response, do not give any explanation.
Do not wrap the JSON in markdown code fences. Output raw JSON only.
Make sure the response is a valid JSON structure, including closing all braces and not including commas after the last element.
Follow this JSON format (don't reuse the values, this is just the format). 'code', 'edges', and 'outputs' are all their own keys, do not nest any of these in another one and make sure to close their braces:
{
"code": {
"UDF1": "code for UDF1 goes here",
"UDF2": "code for UDF2 goes here"
},
"edges": [
["UDF1", "UDF2"]
],
"outputs": {
"UDF1": ["min_values", "max_values", "mean_values", "data"],
"UDF2": ["html-content"]
}
}
Make sure only the keys in the code section appear in the edges and outputs sections. Do not include any extraneous fields.
Do not include any extraneous UDFs in the code field that include empty strings.
Give ALL of the code, do not omit anything or use placeholders for code. Make sure ALL code in the original is translated over.
The value of each UDF must be a valid JSON string: escape newlines, quotes, and backslashes correctly so that the decoded string is runnable R. Prefer single quotes for R strings so fewer characters need escaping.
Convert following the instructions and examples given. Here is the code:
`;

export const R_MAPPING_PROMPT = `
Here is an example of a mapping generated between the given example R code and the Texera UDFs using their CELL and UDF IDs. Cell IDs are designated by the UUID following '# START'. The format should be kept the same.
{
"UDF1": ["CELL3", "CELL4"],
"UDF2": ["CELL5"],
"UDF3": ["CELL6", "CELL7"],
"UDF4": ["CELL8"],
"UDF5": ["CELL9"]
}
Now create a mapping for the UDFs and the original code. Link the code blocks marked by 'START <cell-uuid>' and 'END <cell-uuid>' with the UDF UUIDs. The code between them should be equivalent. Multiple cells can be mapped to the same UDF when that UDF implements the logic of those cells. There could be any number of cells and UDFs, so only create the correct number in the mapping. Only give the mapping.
`;

export const R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_SCRIPT = `
Here is an example of breaking up R code into multiple Texera R UDFs. Format your response structure exactly like the given example. The "code" key contains a dictionary of the UDF IDs with their respective code. The "edges" key contains a list of pairs that contains the connections between UDFs. The "outputs" key contains a dictionary of the UDF IDs with a list of the output column names of the data.frame that the UDF returns. The UDFs can branch, so it does not have to be a linear chain, but each UDF reads from at most one other UDF.

The original code is shown with each line prefixed by its line number and a '|'. Those prefixes are annotations so that line ranges can be referred to later. They are not part of the code and must never appear in the code you generate.

Original Code:
\`\`\`r
 1| library(ggplot2)
 2| library(rpart)
 3| 
 4| # Load the dataset
 5| data <- read.csv('diabetes.csv')
 6| 
 7| # Remove duplicate rows
 8| data <- unique(data)
 9| 
10| # Remove rows with missing values
11| data <- na.omit(data)
12| 
13| # Print the minimum, maximum, and mean for all fields
14| print(sapply(data, min))
15| print(sapply(data, max))
16| print(colMeans(data))
17| 
18| # Boxplot of glucose by outcome
19| ggplot(data, aes(x = factor(Outcome), y = Glucose)) +
20|   geom_boxplot() +
21|   labs(x = 'Outcome', y = 'Glucose', title = 'Glucose by Outcome')
22| 
23| # Split data into training and testing sets (80% train, 20% test)
24| set.seed(42)
25| train_idx <- sample(seq_len(nrow(data)), size = floor(0.8 * nrow(data)))
26| train <- data[train_idx, ]
27| test <- data[-train_idx, ]
28| 
29| # Scale features using the training set center and spread
30| features <- setdiff(names(data), 'Outcome')
31| centers <- sapply(train[features], mean)
32| spreads <- sapply(train[features], sd)
33| train[features] <- scale(train[features], center = centers, scale = spreads)
34| test[features] <- scale(test[features], center = centers, scale = spreads)
35| 
36| # Train logistic regression model
37| glm_model <- glm(Outcome ~ ., data = train, family = binomial)
38| glm_pred <- ifelse(predict(glm_model, test, type = 'response') > 0.5, 1, 0)
39| glm_accuracy <- mean(glm_pred == test$Outcome)
40| print(sprintf('Logistic Regression Accuracy: %.2f%%', 100 * glm_accuracy))
41| 
42| # Train decision tree model
43| tree_model <- rpart(factor(Outcome) ~ ., data = train, method = 'class')
44| tree_pred <- predict(tree_model, test, type = 'class')
45| tree_accuracy <- mean(as.character(tree_pred) == as.character(test$Outcome))
46| print(sprintf('Decision Tree Accuracy: %.2f%%', 100 * tree_accuracy))
\`\`\`

Texera UDF conversion:
\`\`\`json
{
    "code": {
        "UDF1": "# UDF1\\nfunction(table, port) {\\n  # Remove duplicate rows\\n  data <- unique(table)\\n\\n  # Remove rows with missing values\\n  data <- na.omit(data)\\n\\n  # Calculate statistics\\n  min_values <- sapply(data, min)\\n  max_values <- sapply(data, max)\\n  mean_values <- colMeans(data)\\n\\n  # One row of serialized objects for the downstream UDFs\\n  data.frame(\\n    min_values = I(list(serialize(min_values, NULL))),\\n    max_values = I(list(serialize(max_values, NULL))),\\n    mean_values = I(list(serialize(mean_values, NULL))),\\n    data = I(list(serialize(data, NULL)))\\n  )\\n}",
        "UDF2": "# UDF2\\nlibrary(ggplot2)\\nlibrary(plotly)\\n\\nfunction(table, port) {\\n  data <- unserialize(table$data[[1]])\\n\\n  if (nrow(data) == 0) {\\n    html <- '<h1>Boxplot is not available.</h1><p>Reason is: input table is empty.</p>'\\n    return(data.frame('html-content' = html, check.names = FALSE))\\n  }\\n\\n  # Boxplot of glucose by outcome\\n  p <- ggplot(data, aes(x = factor(Outcome), y = Glucose)) +\\n    geom_boxplot() +\\n    labs(x = 'Outcome', y = 'Glucose', title = 'Glucose by Outcome')\\n\\n  # Convert the plot to an HTML page that loads Plotly from a CDN\\n  json <- plotly_json(ggplotly(p), jsonedit = FALSE)\\n  html <- paste0(\\n    '<html><head><script src=\\"https://cdn.plot.ly/plotly-2.35.2.min.js\\"></script></head>',\\n    '<body><div id=\\"plot\\" style=\\"width:100%;height:100%\\"></div><script>',\\n    'var fig = ', json, '; Plotly.newPlot(\\"plot\\", fig.data, fig.layout);',\\n    '</script></body></html>'\\n  )\\n  data.frame('html-content' = html, check.names = FALSE)\\n}",
        "UDF3": "# UDF3\\nfunction(table, port) {\\n  data <- unserialize(table$data[[1]])\\n\\n  # Split data into training and testing sets (80% train, 20% test)\\n  set.seed(42)\\n  train_idx <- sample(seq_len(nrow(data)), size = floor(0.8 * nrow(data)))\\n  train <- data[train_idx, ]\\n  test <- data[-train_idx, ]\\n\\n  # Scale features using the training set center and spread\\n  features <- setdiff(names(data), 'Outcome')\\n  centers <- sapply(train[features], mean)\\n  spreads <- sapply(train[features], sd)\\n  train[features] <- scale(train[features], center = centers, scale = spreads)\\n  test[features] <- scale(test[features], center = centers, scale = spreads)\\n\\n  data.frame(\\n    train = I(list(serialize(train, NULL))),\\n    test = I(list(serialize(test, NULL)))\\n  )\\n}",
        "UDF4": "# UDF4\\nfunction(table, port) {\\n  train <- unserialize(table$train[[1]])\\n  test <- unserialize(table$test[[1]])\\n\\n  # Train logistic regression model\\n  glm_model <- glm(Outcome ~ ., data = train, family = binomial)\\n  glm_pred <- ifelse(predict(glm_model, test, type = 'response') > 0.5, 1, 0)\\n  glm_accuracy <- mean(glm_pred == test$Outcome)\\n\\n  # No UDF reads this one, so its columns are character\\n  data.frame(glm_accuracy = sprintf('Logistic Regression Accuracy: %.2f%%', 100 * glm_accuracy))\\n}",
        "UDF5": "# UDF5\\nlibrary(rpart)\\n\\nfunction(table, port) {\\n  train <- unserialize(table$train[[1]])\\n  test <- unserialize(table$test[[1]])\\n\\n  # Train decision tree model\\n  tree_model <- rpart(factor(Outcome) ~ ., data = train, method = 'class')\\n  tree_pred <- predict(tree_model, test, type = 'class')\\n  tree_accuracy <- mean(as.character(tree_pred) == as.character(test$Outcome))\\n\\n  # No UDF reads this one, so its columns are character\\n  data.frame(tree_accuracy = sprintf('Decision Tree Accuracy: %.2f%%', 100 * tree_accuracy))\\n}"
    },
    "edges": [
        [
            "UDF1",
            "UDF2"
        ],
        [
            "UDF1",
            "UDF3"
        ],
        [
            "UDF3",
            "UDF4"
        ],
        [
            "UDF3",
            "UDF5"
        ]
    ],
    "outputs": {
        "UDF1": [
            "min_values",
            "max_values",
            "mean_values",
            "data"
        ],
        "UDF2": [
            "html-content"
        ],
        "UDF3": [
            "train",
            "test"
        ],
        "UDF4": [
            "glm_accuracy"
        ],
        "UDF5": [
            "tree_accuracy"
        ]
    }
}
\`\`\``;

export const R_SCRIPT_WORKFLOW_PROMPT = `You are an expert in R coding and workflow systems.
Many users of Texera system are non-technical, but the R scripts they provide are written by technical people.
They want to convert their R scripts to Texera workflows.
Your goal is to help convert these R scripts into a Texera workflow that non-technical users can use directly.
So do not remove or modify any user-defined functions, preserve their names and structure as they are.
Ensure that all essential logic remains intact.
Create multiple Texera R UDF codes using the provided R code.
Number each UDF, starting at 1 and incrementing, by starting with a comment that states that UDF number.

Every UDF uses the Table API: its code ends with one function(table, port) that takes a data.frame and returns a data.frame.
Do not use the Tuple API or coro generators.
Load every package a UDF uses with library() before its function. Do not call install.packages().

Do not load data from a file in the first UDF; the workflow's source operator supplies the initial data,
so assume it is already given to you as the table argument. Replacing file-loading code with this input
is the one exception to preserving all original code (see below).
Separate operators as if they will run in different R sessions: a UDF sees only the table it receives.

Each UDF has exactly one output, so follow the data passing rules:
a UDF whose output another UDF reads returns one row where every column is a serialized R object,
and a UDF that no other UDF reads returns character columns.
Ensure all information is passed on (including training and testing data) if subsequent UDFs need it.
Each UDF has one input. When a step needs results from several earlier UDFs, carry those results forward
through a single chain instead of connecting several UDFs into one.
Values the original code prints become columns of the returned data.frame.
Plots follow the visualization rules and return an html-content column.

It is VERY important that all of the original code in the R script is represented in the generated workflow.
Make sure that nothing in the original is removed and that the semantic meaning of what the original code was doing is retained.
The only exception is data-loading code (e.g. read.csv); it is represented by the workflow's input/source operator rather than copied into a UDF.
If there are user-defined R functions, include the entire function definition in every UDF that calls it.

Return only the JSON formatted response, do not give any explanation.
Do not wrap the JSON in markdown code fences. Output raw JSON only.
Make sure the response is a valid JSON structure, including closing all braces and not including commas after the last element.
Follow this JSON format (don't reuse the values, this is just the format). 'code', 'edges', and 'outputs' are all their own keys, do not nest any of these in another one and make sure to close their braces:
{
"code": {
"UDF1": "code for UDF1 goes here",
"UDF2": "code for UDF2 goes here"
},
"edges": [
["UDF1", "UDF2"]
],
"outputs": {
"UDF1": ["min_values", "max_values", "mean_values", "data"],
"UDF2": ["html-content"]
}
}
Make sure only the keys in the code section appear in the edges and outputs sections. Do not include any extraneous fields.
Do not include any extraneous UDFs in the code field that include empty strings.
Give ALL of the code, do not omit anything or use placeholders for code. Make sure ALL code in the original is translated over.
The value of each UDF must be a valid JSON string: escape newlines, quotes, and backslashes correctly so that the decoded string is runnable R. Prefer single quotes for R strings so fewer characters need escaping.
Each line of the script below is prefixed with its line number followed by '| '. Those prefixes are annotations
so that line ranges can be referred to later; never reproduce them in any generated UDF code.
Convert following the instructions and examples given. Here is the code:
`;

export const R_SCRIPT_MAPPING_PROMPT = `
Here is an example of a mapping generated between the given example R code and the Texera UDFs, using line ranges of the original script and the UDF IDs. A range is a pair [firstLine, lastLine]; both bounds are 1-indexed and inclusive, and they refer to the line numbers shown in the prefix of the original code. A UDF may list several ranges when its logic came from separate parts of the script. The format should be kept the same.
{
"UDF1": [[7, 16]],
"UDF2": [[18, 21]],
"UDF3": [[23, 34]],
"UDF4": [[36, 40]],
"UDF5": [[42, 46]]
}
Now create a mapping for the UDFs and the original code you were given. For each UDF, report the line ranges of the original script whose logic that UDF implements. The code in those lines should be equivalent to what the UDF does. Lines that no UDF implements, such as library() calls or the data loading that the workflow's source operator replaces, can be left out entirely. Give the first line before the last within each range, and do not shift the numbers: they must match the prefixes you were shown. There could be any number of ranges and UDFs, so only create the correct number in the mapping. Only give the mapping.
`;


# Getting Started


* For users, visit [Step 0 - Guide to Use Texera](https://github.com/Texera/texera/wiki/Guide-for-how-to-use-Texera).
* For developers, visit [Step 1 - Guide to Develop Texera](https://github.com/Texera/texera/wiki/Guide-for-Developers).

***

# Implementing an Operator
Texera supports operators in Java and Python natively. Additionally, Python operators can be implemented as a Python UDF for a quick extension.
* [Step 2 - Guide to Implement a Java Native Operator](https://github.com/Texera/texera/wiki/Guide-to-Implement-a-Java-Native-Operator)
* [Step 3 - Guide to Use a Python UDF](https://github.com/Texera/texera/wiki/Guide-to-Use-a-Python-UDF)
* [Step 4 - Guide to Implement a Python Native Operator (converting from a Python UDF)](https://github.com/Texera/texera/wiki/Guide-to-Implement-a-Python-Native-Operator-(converting-from-a-Python-UDF))

***
# Contributing to the Project
## 1. Create an Issue for Any Proposed Change
To track any proposed changes to the codebase, developers should first create a corresponding issue. While we often have offline discussions, any such conversation that involves proposed code changes should be summarized and added to the relevant issue. Developers are free to use their preferred tools to create diagrams or documents, but the final outputs should be attached to the issue.

## 2. Include Final Design in the Pull Request (PR) Description

[Step 5 - Guide to Raise a Pull Request (PR)](https://github.com/Texera/texera/blob/master/CONTRIBUTING.md)

If a design document exists for a particular feature or change, it should be included or summarized in the PR description. This ensures the context and rationale for changes are clearly visible during the review process.

## 3. Store User-Facing Documentation in the docs Folder

Any documentation intended to help users understand or use the system should be added to the docs folder in the main repository.

## 4. Review Process and Merging Guidelines

Reviewers are responsible for ensuring that each PR aligns with the design discussed in the associated issue. Any contributor can serve as a reviewer. However, only a committer can merge a PR. While a committer doesn’t have to review every PR personally, they must verify that the PR adheres to the Code Contribution Process before merging.

## 5. Sync Discussions Across Channels

All development-related discussions from the Texera Slack #dev channel and Texera GitHub repo should be synced to this dev mailing list to ensure transparency and continuity.

***
# Available Visualization Operator tasks
* Plotly, a Python graphing library, has a list of basic charts, which can be found at [Plotly Python Open Source Graphing Library Basic Charts](https://plotly.com/python/basic-charts/). By following the 6 steps, you should have enough information about implementing a new Python Native Operator and raise a PR successfully. Currently, we implemented several visualization operators for the them: Scatter Plots, Line Charts, Bar Charts (Horizontal as well), Pie Charts, Bubble Charts, Dots Plots, Filled Area Plots, Gantt Chart, Hierarchy Chart (TreeMap and Sunburst). To check the latest implemented operators, please refers to [this link](https://github.com/apache/texera/tree/main/common/workflow-operator/src/main/scala/org/apache/texera/amber/operator/visualization) that contains all the existing visualization operators.

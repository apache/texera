---
title: "Guide for how to use Texera"
weight: 10
---

Texera is an open-source system that supports collaborative data science at scale using Web-based workflows. This page includes instructions on how to install the system as a developer and do a simple workflow.

## Prerequisites
We assume you either went through [Installing Apache Texera using Docker](/docs/getting-started/installing-using-docker/), or the [Guide for Texera Developers](/docs/contribution-guidelines/guide-for-developers/). And Texera is up-and-running on your laptop.

## Access Texera through Browser
Enter Texera's URL on your browser to access Texera.

An admin account with username `texera` and password `texera` is pre-created by default. Input the username, password and click the `Sign in` button to login as the admin:
![Screenshot 2025-06-16 at 3 43 02 PM](/images/github-assets/ebcb10f7-3d25-4673-b908-7e2de2d3179e.png)

### User Dashboard UI Overview
Once logged in, you should see the below page:
![Screenshot 2025-06-16 at 3 45 38 PM](/images/github-assets/baa3d599-99c6-401f-a772-4300cbc7c9a9.png)

This is Texera's dashboard page. On the left navigation bar, you can switch between different resource modules, including 
- `Workflows` for workflow management 
- `Datasets` for dataset management
- `Quota` for checking the usage statistics
- `Admin` for managing users on the Texera system. This tab is only visible for system admins.

### Workflow Workspace UI Overview

![Screenshot 2025-06-16 at 3 52 40 PM](/images/github-assets/605df038-5335-4a65-bcf8-fa872ce79551.png)

1. **Operator Library/Menu**: 

   It is separated into multiple dropdown menus based on the operator type, e.g., Source Operator, Search Operator, etc. You can drag and drop an operator from these dropdown menus onto the Workflow Canvas.

2. **Workflow Canvas**:

   It is the main playground, where you can drag and drop Operators from the Operator Library onto it. Each operator is shown as a square box and connected with other operators with arrowed links which indicates the data flow.

3. **Properties Editor Panel**:

   The panel will show up when you highlight a specific operator (by clicking on it) in the Workflow Canvas. You can customize the properties of the selected operator, for example, set the keyword for a filter. When the selected operator is configured correctly, a green ring will surround it; while a red ring usually indicates an error in configuration or connection to other operators.

4. **Result Panel**:

   By default or when there is no result, it is hidden. You can click on the little UP arrow to expand this panel. When a workflow is finished running, the result panel will pop up with the data. You may slide up and down or left and right to view the data inside the panel.  


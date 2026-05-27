---
title: "Gantt Chart"
description: "A Gantt chart is a type of bar chart that illustrates a project schedule. The chart lists the tasks to be performed on the vertical axis, and time intervals on the horizontal axis. The width of the horizontal bars in the graph shows the duration of each activity."
category: "Basic"
operator_type: "GanttChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Pattern |  | String | - | Add texture to the chart based on an attribute |
| Start Datetime Column | ✓ | String (timestamp) | - | The start timestamp of the task |
| Finish Datetime Column | ✓ | String (timestamp) | - | The end timestamp of the task |
| Task Column | ✓ | String | - | The name of the task |
| Color Column |  | String | - | Column to color tasks |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |

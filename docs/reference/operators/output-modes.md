---
title: "Output Port Modes"
description: "Reference for operator output port modes"
weight: 12
categories: [Reference]
tags: [reference]
---

[Home](../)

Texera operators emit data through output ports. Each port advertises a **mode** that describes how downstream operators should interpret the stream of tuples it produces.

### Set Snapshot

The port **re-emits the complete result set on each update**. Downstream operators always see the full materialized result.

### Delta Updates

The port emits an **incremental delta of the result set on each update**. Downstream operators apply the delta on top of prior state instead of receiving a re-materialized snapshot.

### Single Snapshot

The port emits **exactly one snapshot for the entire execution** (not per update). Used for visualization operators whose output may exceed the memory limit, making repeated full-snapshot emission impractical.


---
title: Reference
description: In-depth technical and configuration references for Texera’s components and environment.
weight: 50
---

{{% pageinfo %}}
This section contains detailed, low-level reference materials for Texera’s configuration, components, and internal modules.
{{% /pageinfo %}}

The **Reference** section provides look-up documentation for developers and maintainers who need specific, technical information about Texera’s internals or environment.  
Unlike the [Concepts](/docs/concepts/) section, which explains *how Texera works*, this section focuses on *how Texera is configured, built, and extended*.

---

### What you’ll find here

This section includes reference information for:

- **Configuration and Environment Setup:** Detailed parameters and environment variables used for development, deployment, and testing.
- **Project Structure:** Explanation of major code directories, module dependencies, and naming conventions.
- **Execution Engine Details:** Low-level reference for engine modules, operators’ lifecycle, and workflow translation.
- **Operator Framework:** Technical notes on operator registration, metadata, and extension mechanisms.
- **Frontend Components:** Descriptions of UI module structure, Angular components, and visualization hooks.
- **Persistence and Storage:** Information about Texera’s internal storage models, catalog, and workflow metadata.

---

### When to use this section

Use this section when you need:

- To understand or modify Texera’s **internal modules or configuration files**.
- To **debug, extend, or refactor** parts of the codebase.
- To **deploy Texera** in a local, testing, or production environment and need to adjust settings or dependencies.

---

### How to maintain this section

Reference pages are often technical and version-specific. Keep them up to date by:

- Linking or embedding **auto-generated documentation** from code comments (e.g., Javadoc for backend modules or TypeDoc for frontend).
- Including **manual reference pages** for configuration files, startup scripts, and architecture diagrams.
- Updating this section whenever internal modules or configuration formats change.

---

### Suggested subpages

| File | Purpose |
|------|----------|
| `reference/configuration.md` | Environment variables, ports, and server settings. |
| `reference/project-structure.md` | Directory overview and build system explanation. |
| `reference/engine.md` | Detailed explanation of execution engine internals. |
| `reference/operators/` | Built-in operator catalog, grouped by category. |
| `reference/frontend.md` | Frontend architecture and components. |
| `reference/storage.md` | Persistence layer, catalog, and metadata handling. |

---

This section is meant to be a **developer’s technical handbook** for Texera’s internal systems — a precise reference for anyone maintaining, extending, or deploying the platform.

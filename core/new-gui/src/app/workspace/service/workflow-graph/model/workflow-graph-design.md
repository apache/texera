## Workflow Graph Services Design Doc

Workflow Graph is the main model of the application - it represents the logical DAG (directed acyclic graph) composing the workflow plan.
Workflow Graph module contains a group of services that work together to manage the graph and handle the changes to it.

#### Overview of Texera's Workflow Graph

The class `WorkflowGraph` defined in `workflow-graph.ts` implements the graph that holds workflow plan that Texera needed.
Texera backend can run the workflow plan using the information in the `WorkflowGraph`.

Each operator has several attributes, such as:

- a unique operator ID
- operator type
- texera-specific properties
- input/output ports
  Each link has:
- a unique link ID
- the source operator+port
- the target operator+port

#### Overview of JointJS's Graph (joint.dia.Graph)

We use JointJS library to display the graph (operators and links) and let the user to manipulate the graph on the UI.

JointJS has its own model `jointGraph: joint.dia.Graph` containing the information needed by JointJS to display them.
The model object `jointGraph` is two-way binded with the view object `jointPaper: joint.dia.Paper` by JointJS.
Whenever the View `jointPaper` is changed by the user, the model `jointGraph` is also automatically changed,
and corresponding events will be emitted. Changing the model can automatically cause the view to be changed accordingly.

#### Overview of classes in Workflow Graph Services

We maintain two separate models:

- `jointGraph`, representing the graph in the UI by JointJS, and `texeraGraph`
- `texeraGraph`, representing the logical DAG workflow (plan) for Texera
  These two models needs to be in sync, and we want to expose a uniform way to change the model and listen to events.
  The operator IDs and the link IDs in `texeraGraph` and `jointGraph` are the same for all operators and links.

The following classes work together to achieve the sync of two models:

- `WorkflowActionServie`: provides the entry points for `actions` on the graph, such as add/delete operator, add/delete link, etc..
- `TexeraGraph`: maintains the actual graph data (operators and links) and provides getters/setters, and event streams
- `JointGraphWrapper`: wraps jointGraph and provides getters and event streams in RxJS Observables (instead of callback functions)
- `SyncTexeraModel`: subscribes to changes event streams from `JointGraphWrapper` and sync `TexeraGraph` accordingly

#### How to use the services and classes from external modules

If an external module wants to:

- change the workflow graph: (add/remove operator, add/remove link, change operator property)
  - call specific actions in `WorkflowActionServie`, jointGraph and texeraGraph will be changed in sync automatically
- read data or listen to events of texera logical workflow graph:
  - from `WorkflowActionServie`, get a read-only version of the texera graph `TexeraGraphReadonly`
  - `hasXXX` and `getXXX` methods, as well as `getXXXEventStream` methods are available
- access UI properties or events from JointJS: (such as the coordinate of an operator, the event of user dragging an operator/link around)
  - get the properties or subscribe to events from `JointGraphWrapper`
  - if more properties or event streams are needed, more getter functions can be created in `JointGraphWrapper`

#### Internal implementation to keep texera and joint graph in sync

Internally, the workflow graph module manages the sync of `JointModelService` and `TexeraModelService` by:

- `WorkflowActionServie` calls corresponding methods for JointGraph and TexeraGraph to make changes

- For `deleteOperator`, `addLink`, and `deleteLink`

  - only call `jointGraph` methods. events will propagate to `syncTexeraModel`, where changes are made
  - these actions can occur either from calling actions in the code, or directly from the UI
  - handlers in `syncTexeraModel` will be triggered regardless the event is from

- For `addOperator` and `setOperatorProperty`
  - call both `jointGraph` methods and `texeraGrpah` methods, `syncTexeraModel` won't handle them
  - `addOperator` could be only triggered inside the code, and we need to pass additional operator data
  - `setOperatorProperty` jointJS doesn't have the notion of changing the property of an operator

`WorkflowActionServie` ---(calls)---> `JointGraph`: `addOperator`, `deleteOperator`, `addLink`, `deleteLink`
`WorkflowActionServie` ---(calls)---> `TexeraGraph`: `addOperator`, `setOperatorProperty`
`SyncTexeraModel` --(subscribes)---> `JointGraph`: ` operatorElementDelete`, `linkCellAdd`, `linkCellDelete`, `linkCellChange`


## Workflow Graph Services Design Doc

Workflow Graph module contains a group of services that work together to handle the changes to the Graph.

Workflow Graph is the main model of the application - it represents the logical DAG (directed acyclic graph) composing the workflow plan.
It consists of operators and link connecting these operators.

The library JointJS is used to display the graph (operators and links) and let the user to manipulate the graph on the UI.
JointJS has its own model `jointGraph: joint.dia.Graph` containing the information needed by JointJS to display them.
The model object `jointGraph` is two-way binded with the view object `jointPaper: joint.dia.Paper` by JointJS.
Whenver the View `jointPaper` is changed by the user, the model `jointGraph` is also automatically changed, 
  and corresponding events will be emitted. Changing the model can automatically cause the view to be changed accordingly.

We maintain two separate models: 
  - `jointGraph`, representing the graph in the UI by JointJS, and `texeraGraph`
  - `texeraGraph`, representing the logical DAG workflow (plan) for Texera
These two models needs to be in Sync, and we want to expose a uniform way to change the model and listen to events.

The following services work together to achieve the sync of two models:
  - `WorkflowActionServie`: provides the entry points for "actions" on the graph, such as add/delete operator, add/delete link, etc..
  - `JointModelService`: handles the JointJS Model `jointGraph`, provides events and properties of JointJS related to the UI
  - `TexeraModelService`: handles the Texera Model `texeraGraph`, provides events of the Texera graph related to the logical workflow

If an external module wants to:
  - change the workflow graph: (add/remove operator, add/remove link, change operator property)
    - call specific actions in `WorkflowActionServie`, jointGraph and texeraGraph will be changed in sync automatically
  - read data from texera workflow graph:
    - get a read-only version of the texera graph from `TexeraModelService`
  - listen to events related to Texera's logical workflow graph:
    - subscribe to Observable event streams in `TexeraModelService`
  - access UI properties or events from JointJS: (such as the coordinate of an operator, the event of user dragging an operator/link around)
    - get the properties or subscribe to events from `JointModelService`

Internally, the workflow graph module manages the sync of `JointModelService` and `TexeraModelService` by:
  - `WorkflowActionServie` provides events corresponding to each action, representing an action is "requested" by the caller in code

  - `JointModelService` subscribes all the action events from `WorkflowActionServie`, whenever an action is called, jointModel is updated accordingly
  - `JointModelService` jointGraph is binded to the view by JointJS, so it also changes with the UI
  - `JointModelService` provides Observable event streams for JointJS events, triggered by the changes to the jointGraph, whether it's from code or direclty from the UI

  - `TexeraModelService` subscribes events from `WorkflowActionServie` or `JointModelService`:
    - Subscribe events in `JointModelService` for `deleteOperator`, `addLink`, `deleteLink`:
      - these actions can occur either from calling actions in the code, or directly from the UI
      - events in `JointModelService` will be triggered regardless the event is from
    - for `addOperator` and `changeOperatorProperty`: subscribes events in `WorkflowActionServie`
      - `addOperator` action could be only triggered inside the code, JointJS cannot add operator on its in the UI
      - we need to pass additional parameters (the data of the operator) to the texeraGraph
      - `changeOperatorProperty` jointJS doesn't have the notion of changing the property of an opeartor

`JointModelService`  ---(subscribes)---> `WorkflowActionServie`: `addOperatorAction`, `deleteOperatorAction`, `addLinkAction`, `deleteLinkAction`
`TexeraModelService` ---(subscribes)---> `WorkflowActionServie`: `addOperatorAction`, `changeOperatorPropertyAction`
`TexeraModelService` ---(subscribes)---> `JointModelService`: `operatorElementDeleted`, `linkCellAdd`, `linkCellDelete`, `linkCellChange`

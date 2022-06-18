import { WorkflowActionService } from "../service/workflow-graph/model/workflow-action.service";

export type commandFuncs =
  | "undoredo"
  | "addOperator"
  | "deleteOperator"
  | "addOperatorsAndLinks"
  | "deleteOperatorsAndLinks"
  | "changeOperatorPosition"
  | "autoLayoutWorkflow"
  | "setOperatorProperty"
  | "addLink"
  | "deleteLink"
  | "deleteLinkWithID"
  | "resetAsNewWorkflow"
  | "setLinkBreakpoint"
  | "disableOperators"
  | "enableOperators"
  | "cacheOperators"
  | "unCacheOperators"
  | "setWorkflowName"
  | "setOperatorCustomName"
  | "highlightOperators"
  | "unhighlightOperators"
  | "highlightLinks"
  | "unhighlightLinks"
  | "addCommentBox"
  | "deleteCommentBox"
  | "changeCommentBoxPosition"
  | "addComment"
  | "editComment"
  | "deleteComment";

// keyof yields permitted property names for T. When we pass function, it'll return value of that function?
// For this type, we index T with the property names for T, which results in us getting the values.
/**
 * type Foo = { a: string, b: number };
 * type ValueOfFoo = ValueOf<Foo>; // string | number
 * ValueOf<Foo> = Foo[a | b] = string | number
 */
type ValueOf<T> = T[keyof T];

// Pick<WorkflowActionService, commandFuncs>: from WorkflowActionService, pick a set of properties whose keys
// are in commandFuncs. commandFuncs are names of functions, so this pick will only allow existing func names.
// So when we make CommandMessage, the function will get inferred from action. Then, it'll require that
// parameters are the parameters for WorkflowActionService[P], or that function.

// P in keyof Pick: P will be one of the properties that exists in there(set of properties from service).
// If we have a name in commandFuncs that doesn't match a property in service, we get error. P picks one of them
export type CommandMessage =
  | ValueOf<{
      [P in keyof Pick<WorkflowActionService, commandFuncs>]: {
        action: P;
        parameters: Parameters<WorkflowActionService[P]>;
        type: string;
      };
    }>
  | ValueOf<{
      [P in keyof Pick<WorkflowActionService, commandFuncs>]: {
        action: P;
        parameters: Parameters<WorkflowActionService[P]>;
        type: string;
      };
    }>;

export interface Command {
  modifiesWorkflow: boolean;

  execute(): void;

  undo?(): void;

  redo?(): void;
}

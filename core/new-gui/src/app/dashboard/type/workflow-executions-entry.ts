export interface WorkflowExecutionsEntry
  extends Readonly<{
    eId: number;
    vId: number;
    startingTime: number;
    completionTime: number;
    status: number;
    result: string;
  }> {}

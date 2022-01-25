export interface WorkflowExecutionsEntry
  extends Readonly<{
    eId: number;
    vId: number;
    startingTime: number;
    completionTime: number;
    status: boolean;
    result: string;
  }> {}

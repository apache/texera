export interface WorkflowExecutionsEntry {
  eId: number;
  vId: number;
  startingTime: number;
  completionTime: number;
  status: number;
  result: string;
  bookmarked: boolean;
}

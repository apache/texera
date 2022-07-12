export interface WorkflowExecutionsEntry {
  eId: number;
  vId: number;
  uId: number;
  name: string;
  startingTime: number;
  completionTime: number;
  status: number;
  result: string;
  bookmarked: boolean;
}

export interface WorkflowExecutionsEntry {
  eId: number;
  vId: number;
  userName: string;
  name: string;
  startingTime: number;
  completionTime: number;
  status: number;
  result: string;
  bookmarked: boolean;
}

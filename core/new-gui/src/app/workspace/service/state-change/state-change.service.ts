import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';

@Injectable({
  providedIn: 'root'
})
export class StateChangeService {

  private status: string = 'finished'; // needs to be changed here
  private stateChangeSubject: Subject<{status: string, operatorID: string}> = new Subject();
  // we first scan all kinds of operators, and the pass the state and operator ID to the workflow flow editor.

  constructor(
    private workflowActionService: WorkflowActionService,
    ) { }


  public stateChangeDetection(): void {
    // send the status of each operator to the workflow editor.
    this.workflowActionService.getTexeraGraph().getAllOperators().forEach (operator =>
      this.stateChangeSubject.next({status: this.status, operatorID: operator.operatorID}));
  }

  public getStateChangeSubjectStream(): Observable<{status: string, operatorID: string}> {
    return this.stateChangeSubject.asObservable();
  }
}

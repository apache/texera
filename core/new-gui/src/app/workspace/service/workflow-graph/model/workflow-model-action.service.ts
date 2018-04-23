import { Observable } from 'rxjs/Observable';
import { Injectable } from '@angular/core';
import { JointModelService } from './jointjs-model.service';
import { TexeraModelService } from './texera-model.service';
import { OperatorPredicate } from '../../../types/workflow-graph';
import { Point } from '../../../types/common.interface';
import { Subject } from 'rxjs/Subject';

@Injectable()
export class WorkflowModelActionService {

  private addOperatorActionSubject: Subject<{ operator: OperatorPredicate, point: Point }> = new Subject();

  private deleteOperatorActionSubject: Subject<{ operatorID: string }> = new Subject();

  constructor(
  ) { }

  public addOperator(operator: OperatorPredicate, point: Point): void {
    this.addOperatorActionSubject.next({ operator, point });
  }

  public onAddOperatorAction(): Observable<{ operator: OperatorPredicate, point: Point }> {
    return this.addOperatorActionSubject.asObservable();
  }

  public deleteOperator(operatorID: string): void {
    this.deleteOperatorActionSubject.next({ operatorID });
  }

  public onDeleteOperatorAction(): Observable<{ operatorID: string }> {
    return this.deleteOperatorActionSubject.asObservable();
  }

}

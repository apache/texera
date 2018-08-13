import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../../common/rxjs-operators.ts';

@Injectable()
export class WorkflowViewEventService {

  public operatorSelectedInEditor = new Subject<{operatorID: string}>();

  public deleteOperatorClickedInEditor = new Subject<{operatorID: string}>();

  // TODO: figure out how to listen to link button deleted event
  // if this feature is needed in the future
  // public deleteLinkClickedInEditor = new Subject<string>();

  public pointerDownOnBlankInEditor = new Subject<{ event: Event, x: number, y: number }>();

  constructor() {
  }

}

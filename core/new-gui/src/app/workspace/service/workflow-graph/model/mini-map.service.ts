import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';
import * as joint from 'jointjs';

/**
 * MiniMapService is used by MiniMapComponent, and just serves as a way
 * for MiniMapComponent to access the WorkflowEditorComponent's model.
 *
 * @author Cynthia Wang
 *
 */
@Injectable({
  providedIn: 'root'
})
export class MiniMapService {

  private miniMapGraphSubject = new Subject<joint.dia.Paper>();

  constructor() { }

   /**
   * This is called in WorkflowEditorComponent, and this just feeds the
   * subject the main workflow's paper, with the intention of using its model.
   */
  public initializeMapPaper(mapPaper: joint.dia.Paper) {
    this.miniMapGraphSubject.next(mapPaper);
  }

  /**
   * This returns an observable for main workflow's paper.
   *
   * MiniMapComponent will call this function to subscribe to this observable and
   * 'listen' for any changes in the main workflow's model, and change the map
   * accordingly.
   */
  public getMiniMapInitializeStream(): Observable<joint.dia.Paper> {
    return this.miniMapGraphSubject.asObservable();
  }
}

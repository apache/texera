import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';
import * as joint from 'jointjs';

@Injectable({
  providedIn: 'root'
})
export class MiniMapService {

  private miniMapGraphSubject = new Subject<joint.dia.Paper>();

  constructor() { }

  public initializeMapPaper(mapPaper: joint.dia.Paper) {
    this.miniMapGraphSubject.next(mapPaper);
  }

  public getMiniMapInitializeStream(): Observable<joint.dia.Paper> {
    return this.miniMapGraphSubject.asObservable();
  }
}

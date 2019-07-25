import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
@Injectable({
  providedIn: 'root'
})
export class OperatorInfoService {
  private operatorInfoSubject: Subject<JSON> = new Subject<JSON>();
  constructor() { }

  public sendOperatorsInfo(info: JSON | undefined) {
    this.operatorInfoSubject.next(info);
  }
  public getOperatorsInfoSubjectStream(): Observable<JSON> {
    return this.operatorInfoSubject.asObservable();
  }
}

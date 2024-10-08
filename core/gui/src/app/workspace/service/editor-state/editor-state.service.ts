import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class EditorStateService {
  private editorStates: Map<string, BehaviorSubject<boolean>> = new Map();

  getEditorState(operatorID: string): Observable<boolean> {
    if (!this.editorStates.has(operatorID)) {
      this.editorStates.set(operatorID, new BehaviorSubject<boolean>(false));
    }
    return this.editorStates.get(operatorID)!.asObservable();
  }

  setEditorState(operatorID: string, isOpen: boolean): void {
    if (!this.editorStates.has(operatorID)) {
      this.editorStates.set(operatorID, new BehaviorSubject<boolean>(isOpen));
    } else {
      this.editorStates.get(operatorID)!.next(isOpen);
    }
  }
}

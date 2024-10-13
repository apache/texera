import { Injectable, ViewContainerRef } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class CodeEditorService {
  public vc!: ViewContainerRef;

  private editorStates: Map<string, BehaviorSubject<boolean>> = new Map();

  /**
   * Returns an observable representing whether the editor for the given operator is open.
   * @param operatorID The ID of the operator.
   * @returns Observable for the editor state.
   */
  getEditorState(operatorID: string): Observable<boolean> {
    if (!this.editorStates.has(operatorID)) {
      this.editorStates.set(operatorID, new BehaviorSubject<boolean>(false));
    }
    return this.editorStates.get(operatorID)!.asObservable();
  }

  /**
   * Sets the editor state for the given operator.
   * @param operatorID The ID of the operator.
   * @param isOpen Whether the editor is open.
   */
  setEditorState(operatorID: string, isOpen: boolean): void {
    if (!this.editorStates.has(operatorID)) {
      this.editorStates.set(operatorID, new BehaviorSubject<boolean>(isOpen));
    } else {
      this.editorStates.get(operatorID)!.next(isOpen);
    }
  }
}

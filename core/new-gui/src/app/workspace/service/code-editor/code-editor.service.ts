import { Injectable, ViewContainerRef } from "@angular/core";

@Injectable({
  providedIn: "root",
})
export class CodeEditorService {
  public vc!: ViewContainerRef;
}

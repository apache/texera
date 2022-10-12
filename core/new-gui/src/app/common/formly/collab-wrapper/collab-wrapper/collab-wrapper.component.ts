import { Component, ElementRef, OnInit, ViewChild } from "@angular/core";
import { FieldWrapper, FormlyFieldConfig } from "@ngx-formly/core";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { merge } from "lodash";
import Quill from "quill";
import * as Y from "yjs";
import { QuillBinding } from "y-quill";
import QuillCursors from "quill-cursors";
import { YText } from "yjs/dist/src/types/YText";

Quill.register("modules/cursors", QuillCursors);

@Component({
  templateUrl: "./collab-wrapper.component.html",
  styleUrls: ["./collab-wrapper.component.css"],
})
export class CollabWrapperComponent extends FieldWrapper implements OnInit {
  private quill?: Quill;
  private currentOperatorId: string = "";
  private operatorType: string = "";
  public propertyKey: string = "";
  private quillBinding?: QuillBinding;
  private sharedText?: Y.Text;
  @ViewChild("editor", { static: true }) divEditor: ElementRef | undefined;

  constructor(private workflowActionService: WorkflowActionService) {
    super();
  }

  ngOnInit(): void {
    if (this.field.key === undefined || this.field.templateOptions === undefined) {
      throw Error(
        `form collab-wrapper field ${this.field} doesn't contain necessary .key and .templateOptions.presetKey attributes`
      );
    } else {
      this.currentOperatorId = this.field.templateOptions.currentOperatorId;
      this.operatorType = this.field.templateOptions.operatorType;
      let parents = [this.field.key];
      let parent = this.field.parent;
      while (parent?.key !== undefined) {
        parents.push(parent.key);
        parent = parent.parent;
      }
      let structure: any = this.workflowActionService
        .getTexeraGraph()
        .getSharedOperatorPropertyType(this.currentOperatorId);
      let parentStructure: any = structure;
      let key: any;
      // console.log(parents);
      while (parents.length > 0) {
        parentStructure = structure;
        key = parents.pop();
        if (structure.constructor.name === "YArray") key = parseInt(key);
        structure = structure.get(key);
      }
      if (structure === undefined && parentStructure !== undefined) {
        if (parentStructure.constructor.name === "YArray") {
          (parentStructure as Y.Array<any>).push([new Y.Text()]);
        } else {
          parentStructure.set(key as string, new Y.Text());
        }
        structure = parentStructure.get(key as string);
      }
      this.sharedText = structure;
      this.registerQuillBinding();
      if (this.currentOperatorId && this.sharedText) {
        this.quillBinding = new QuillBinding(
          this.sharedText,
          this.quill,
          this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
        );
      }
    }
  }

  private registerQuillBinding() {
    // Operator name editor
    const element = this.divEditor as ElementRef;
    this.quill = new Quill(element.nativeElement, {
      modules: {
        cursors: true,
        toolbar: false,
        history: {
          // Local undo shouldn't undo changes
          // from remote users
          userOnly: true,
        },
      },
      placeholder: "Start collaborating...",
      theme: "bubble",
    });
  }

  static setupFieldConfig(mappedField: FormlyFieldConfig, operatorType: string, currentOperatorId: string) {
    const fieldConfig: FormlyFieldConfig = {
      wrappers: ["form-field", "collab-wrapper"],
      templateOptions: {
        operatorType: operatorType,
        currentOperatorId: currentOperatorId,
      },
    };
    merge(mappedField, fieldConfig);
  }
}

import { AfterContentInit, Component, ElementRef, ViewChild } from "@angular/core";
import { FieldWrapper, FormlyFieldConfig } from "@ngx-formly/core";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { merge } from "lodash";
import Quill from "quill";
import * as Y from "yjs";
import { QuillBinding } from "y-quill";
import QuillCursors from "quill-cursors";

Quill.register("modules/cursors", QuillCursors);

@Component({
  templateUrl: "./collab-wrapper.component.html",
  styleUrls: ["./collab-wrapper.component.css"],
})
export class CollabWrapperComponent extends FieldWrapper implements AfterContentInit {
  private quill?: Quill;
  private currentOperatorId: string = "";
  private operatorType: string = "";
  private quillBinding?: QuillBinding;
  private sharedText?: Y.Text;
  @ViewChild("editor", { static: true }) divEditor: ElementRef | undefined;

  constructor(private workflowActionService: WorkflowActionService) {
    super();
  }

  ngAfterContentInit(): void {
    this.setUpYTextEditor();
    this.formControl.valueChanges.subscribe(value => {
      if (this.sharedText !== undefined && value !== this.sharedText.toJSON()) {
        this.setUpYTextEditor();
      }
    });
  }

  private setUpYTextEditor() {
    setTimeout(() => {
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
        let parentStructure: any = this.workflowActionService
          .getTexeraGraph()
          .getSharedOperatorPropertyType(this.currentOperatorId);
        let structure: any = undefined;
        let key: any;
        this.workflowActionService.getTexeraGraph().bundleActions(() => {
          while (parents.length > 0 && parentStructure !== undefined && parentStructure !== null) {
            key = parents.pop();
            structure = parentStructure.get(key);
            if (structure === undefined || structure === null) {
              if (parents.length > 0) {
                if (parentStructure.constructor.name === "YArray") {
                  const yArray = parentStructure as Y.Array<any>;
                  if (yArray.length > parseInt(key)) {
                    yArray.delete(parseInt(key), 1);
                    yArray.insert(parseInt(key), [new Y.Map<any>()]);
                  } else {
                    yArray.push([new Y.Map<any>()]);
                  }
                } else {
                  parentStructure.set(key as string, new Y.Map<any>());
                }
              } else {
                if (parentStructure.constructor.name === "YArray") {
                  const yArray = parentStructure as Y.Array<any>;
                  if (yArray.length > parseInt(key)) {
                    yArray.delete(parseInt(key), 1);
                    yArray.insert(parseInt(key), [new Y.Text("")]);
                  } else {
                    yArray.push([new Y.Text("")]);
                  }
                } else {
                  parentStructure.set(key as string, new Y.Text());
                }
              }
              structure = parentStructure.get(key);
            }
            parentStructure = structure;
          }
        });
        this.sharedText = structure;
        this.initializeQuillEditor();
        if (this.currentOperatorId && this.sharedText) {
          this.quillBinding = new QuillBinding(
            this.sharedText,
            this.quill,
            this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
          );
        }
      }
    }, 100);
  }

  private initializeQuillEditor() {
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
        // Disable newline on enter and instead quit editing
        keyboard:
          this.field.type === "textarea"
            ? {}
            : {
                bindings: {
                  enter: {
                    key: 13,
                    handler: () => {},
                  },
                  shift_enter: {
                    key: 13,
                    shiftKey: true,
                    handler: () => {},
                  },
                },
              },
      },
      formats: [],
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

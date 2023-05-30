import { AfterContentInit, Component, ElementRef, ViewChild } from "@angular/core";
import { FieldWrapper, FormlyFieldConfig } from "@ngx-formly/core";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { merge } from "lodash";
import Quill from "quill";
import * as Y from "yjs";
import { QuillBinding } from "y-quill";
import QuillCursors from "quill-cursors";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

// Quill related definitions
export const COLLAB_DEBOUNCE_TIME_MS = 10;
const Clipboard = Quill.import("modules/clipboard");
const Delta = Quill.import("delta");

/**
 * Custom clipboard module that removes rich text formats and newline characters
 */
class PlainClipboard extends Clipboard {
  onPaste(e: { preventDefault: () => void; clipboardData: { getData: (arg0: string) => any } }) {
    e.preventDefault();
    const range = this.quill.getSelection();
    const text = (e.clipboardData.getData("text/plain") as string).replace(/\n/g, "");
    const delta = new Delta().retain(range.index).delete(range.length).insert(text);
    const index = text.length + range.index;
    const length = 0;
    this.quill.updateContents(delta, "silent");
    this.quill.setSelection(index, length, "silent");
    this.quill.scrollIntoView();
  }
}

Quill.register(
  {
    "modules/clipboard": PlainClipboard,
  },
  true
);

Quill.register("modules/cursors", QuillCursors);

/**
 * CollabWrapperComponent is a custom field wrapper that connects a string/textfield typed form field to a collaborative
 * text editor based on Yjs and Quill.
 */
@UntilDestroy()
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
    this.formControl.valueChanges.pipe(untilDestroyed(this)).subscribe(value => {
      if (this.sharedText !== undefined && value !== this.sharedText.toJSON()) {
        this.setUpYTextEditor();
      }
    });
    this.registerDisableEditorInteractivityHandler();
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
    }, COLLAB_DEBOUNCE_TIME_MS);
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
    this.quill.enable(this.evaluateInteractivity());
  }

  private evaluateInteractivity(): boolean {
    return this.formControl.enabled;
  }

  private setInteractivity(interactive: boolean) {
    if (interactive !== this.quill?.isEnabled()) this.quill?.enable(interactive);
  }

  private registerDisableEditorInteractivityHandler(): void {
    this.formControl.statusChanges.pipe(untilDestroyed(this)).subscribe(_ => {
      this.setInteractivity(this.evaluateInteractivity());
    });
  }

  static setupFieldConfig(
    mappedField: FormlyFieldConfig,
    operatorType: string,
    currentOperatorId: string,
    includePresetWrapper: boolean = false
  ) {
    const fieldConfig: FormlyFieldConfig = {
      wrappers: includePresetWrapper
        ? ["form-field", "preset-wrapper", "collab-wrapper"]
        : ["form-field", "collab-wrapper"],
      templateOptions: {
        operatorType: operatorType,
        currentOperatorId: currentOperatorId,
      },
    };
    merge(mappedField, fieldConfig);
  }
}

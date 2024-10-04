// import { AfterViewInit, Component, ComponentRef, ElementRef, OnDestroy, ViewChild } from "@angular/core";
// import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
// import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
// import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
// import { YText } from "yjs/dist/src/types/YText";
// import { MonacoBinding } from "y-monaco";
// import { Subject, take } from "rxjs";
// import { takeUntil } from "rxjs/operators";
// import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
// import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
// import { Coeditor } from "../../../common/type/user";
// import { YType } from "../../types/shared-editing.interface";
// import { FormControl } from "@angular/forms";
// import { AIAssistantService, TypeAnnotationResponse } from "../../service/ai-assistant/ai-assistant.service";
// import { AnnotationSuggestionComponent } from "./annotation-suggestion.component";

// import { MonacoEditorLanguageClientWrapper, UserConfig } from 'monaco-editor-wrapper';
// import { WebSocketMessageReader, WebSocketMessageWriter, toSocket } from 'vscode-ws-jsonrpc';
// import * as monaco from 'monaco-editor';

// @UntilDestroy()
// @Component({
//   selector: "texera-code-editor",
//   templateUrl: "code-editor.component.html",
//   styleUrls: ["code-editor.component.scss"],
// })
// export class CodeEditorComponent implements AfterViewInit, OnDestroy {
//   @ViewChild("editor", { static: true }) editorElement!: ElementRef;
//   @ViewChild("container", { static: true }) containerElement!: ElementRef;
//   @ViewChild(AnnotationSuggestionComponent) annotationSuggestion!: AnnotationSuggestionComponent;
//   private code?: YText;
//   private editor?: monaco.editor.IStandaloneCodeEditor;
//   private workflowVersionStreamSubject: Subject<void> = new Subject<void>();
//   private operatorID!: string;
//   public title: string | undefined;
//   public formControl!: FormControl;
//   public componentRef: ComponentRef<CodeEditorComponent> | undefined;
//   public language: string = "";
//   public languageTitle: string = "";

//   private wrapper?: MonacoEditorLanguageClientWrapper;

//   // Boolean to determine whether the suggestion UI should be shown
//   public showAnnotationSuggestion: boolean = false;
//   // The code selected by the user
//   public currentCode: string = "";
//   // The result returned by the backend AI assistant
//   public currentSuggestion: string = "";
//   // The range selected by the user
//   public currentRange: monaco.Range | undefined;
//   public suggestionTop: number = 0;
//   public suggestionLeft: number = 0;
//   // For "Add All Type Annotation" to show the UI individually
//   private userResponseSubject?: Subject<void>;
//   private isMultipleVariables: boolean = false;
//   private componentDestroy = new Subject<void>();

//   private generateLanguageTitle(language: string): string {
//     return `${language.charAt(0).toUpperCase()}${language.slice(1)} UDF`;
//   }

//   changeLanguage(newLanguage: string) {
//     this.language = newLanguage;
//     console.log("change to ", newLanguage);
//     const model = this.editor?.getModel();
//     if (model) {
//       monaco.editor.setModelLanguage(model, newLanguage);
//     }
//     this.languageTitle = this.generateLanguageTitle(newLanguage);
//   }

//   constructor(
//     private sanitizer: DomSanitizer,
//     private workflowActionService: WorkflowActionService,
//     private workflowVersionService: WorkflowVersionService,
//     public coeditorPresenceService: CoeditorPresenceService,
//     private aiAssistantService: AIAssistantService
//   ) {
//     const currentOperatorId = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
//     const operatorType = this.workflowActionService.getTexeraGraph().getOperator(currentOperatorId).operatorType;

//     if (operatorType === "RUDFSource" || operatorType === "RUDF") {
//       this.changeLanguage("r");
//     } else if (
//       operatorType === "PythonUDFV2" ||
//       operatorType === "PythonUDFSourceV2" ||
//       operatorType === "DualInputPortsPythonUDFV2"
//     ) {
//       this.changeLanguage("python");
//     } else {
//       this.changeLanguage("java");
//     }
//   }

//   async ngAfterViewInit(): Promise<void> {
//     this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", true);
//     this.operatorID = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
//     this.title = this.workflowActionService.getTexeraGraph().getOperator(this.operatorID).customDisplayName;
//     const style = localStorage.getItem(this.operatorID);
//     if (style) this.containerElement.nativeElement.style.cssText = style;
//     this.code = (
//       this.workflowActionService
//         .getTexeraGraph()
//         .getSharedOperatorType(this.operatorID)
//         .get("operatorProperties") as YType<Readonly<{ [key: string]: any }>>
//     ).get("code") as YText;

//     console.log("added this code ", this.code);

//     this.workflowVersionService
//       .getDisplayParticularVersionStream()
//       .pipe(takeUntil(this.workflowVersionStreamSubject))
//       .subscribe(async (displayParticularVersion: boolean) => {
//         if (displayParticularVersion) {
//           this.initDiffEditor();
//         } else {
//           await this.initMonaco();
//           this.formControl.statusChanges.pipe(untilDestroyed(this)).subscribe(() => {
//             const editorInstance = this.wrapper?.getEditor();
//             if (editorInstance) {
//               editorInstance.updateOptions({
//                 readOnly: this.formControl.disabled,
//               });
//             }
//           });
//         }
//       });
//   }

//   // **
//   //  * Specify the co-editor's cursor style. This step is missing from MonacoBinding.
//   //  * @param coeditor
//   //  */
//   public getCoeditorCursorStyles(coeditor: Coeditor) {
//     const textCSS =
//       "<style>" +
//       `.yRemoteSelection-${coeditor.clientId} { background-color: ${coeditor.color?.replace("0.8", "0.5")}}` +
//       `.yRemoteSelectionHead-${coeditor.clientId}::after { border-color: ${coeditor.color}}` +
//       `.yRemoteSelectionHead-${coeditor.clientId} { border-color: ${coeditor.color}}` +
//       "</style>";
//     return this.sanitizer.bypassSecurityTrustHtml(textCSS);
//   }

//   async ngOnDestroy(): Promise<void> {
//     this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", false);
//     localStorage.setItem(this.operatorID, this.containerElement.nativeElement.style.cssText);

//     if (this.wrapper !== undefined) {
//       await this.wrapper.dispose()
//     }

//     if (this.workflowVersionStreamSubject) {
//       this.workflowVersionStreamSubject.next();
//       this.workflowVersionStreamSubject.complete();
//     }
//   }

//   /**
//    * Create a Monaco editor and connect it to MonacoBinding.
//    * @private
//    */
//   private async initMonaco() {

//     if (!this.wrapper) {
//       this.wrapper = new MonacoEditorLanguageClientWrapper();

//       const userConfig: UserConfig = {
//         wrapperConfig: {
//           editorAppConfig: {
//             $type: 'extended',
//             codeResources: {
//               main: {
//                 text: this.code ? this.code.toString() : 'print("Hello, World!")',
//                 uri: `innnn-memory-${this.operatorID}.py` // 使用 operatorID 作为唯一标识
//               }
//             },
//             userConfiguration: {
//               json: JSON.stringify({
//                 'workbench.colorTheme': 'Default Dark Modern',
//               })
//             }
//           }
//         },
//         languageClientConfig: {
//           languageId: this.language || 'python',  // 默认使用 'python' 语言
//           options: {
//             $type: 'WebSocketUrl',
//             url: 'ws://localhost:3000/language-server',
//             startOptions: {
//               onCall: () => {
//                 console.log('Language client started');
//               },
//               reportStatus: true,
//             }
//           }
//         }
//       };

//       try {
//         await this.wrapper.initAndStart(userConfig, this.editorElement.nativeElement);

//         this.formControl.statusChanges.subscribe(() => {
//           const editorInstance = this.wrapper?.getEditor();
//           if (editorInstance) {
//             editorInstance.updateOptions({
//               readOnly: this.formControl.disabled,
//             });
//           }
//         });

//       } catch (e) {
//         console.error('Error during Monaco Editor initialization:', e);
//       }
//     }

//     const editor = this.wrapper?.getEditor();

//     if (this.code && editor) {
//       new MonacoBinding(
//         this.code,
//         editor.getModel()!,
//         new Set([editor]),
//         this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
//       );
//     }
//     this.editor = editor;

//     // Check if the AI provider is "openai"
//     this.aiAssistantService
//       .checkAIAssistantEnabled()
//       .pipe(untilDestroyed(this))
//       .subscribe({
//         next: (isEnabled: string) => {
//           if (isEnabled === "OpenAI") {
//             // "Add Type Annotation" Button
//             editor?.addAction({
//               id: "type-annotation-action",
//               label: "Add Type Annotation",
//               contextMenuGroupId: "1_modification",
//               contextMenuOrder: 1.0,
//               run: ed => {
//                 const selection = ed.getSelection();
//                 const model = ed.getModel();
//                 if (!model || !selection) {
//                   return;
//                 }
//                 const code = model.getValueInRange(selection);
//                 const lineNumber = selection.startLineNumber;
//                 const allcode = model.getValue();
//                 this.handleTypeAnnotation(
//                   code,
//                   selection,
//                   ed as monaco.editor.IStandaloneCodeEditor,
//                   lineNumber,
//                   allcode
//                 );
//               },
//             });
//           }

//           editor?.addAction({
//             id: "all-type-annotation-action",
//             label: "Add All Type Annotations",
//             contextMenuGroupId: "1_modification",
//             contextMenuOrder: 1.1,
//             run: ed => {
//               const selection = ed.getSelection();
//               const model = ed.getModel();
//               if (!model || !selection) {
//                 return;
//               }

//               const selectedCode = model.getValueInRange(selection);
//               const allCode = model.getValue();

//               this.aiAssistantService
//                 .locateUnannotated(selectedCode, selection.startLineNumber)
//                 .pipe(takeUntil(this.componentDestroy))
//                 .subscribe(variablesWithoutAnnotations => {
//                   if (variablesWithoutAnnotations.length === 0) {
//                     return;
//                   }

//                   let offset = 0;
//                   let lastLine: number | undefined;

//                   this.isMultipleVariables = true;
//                   this.userResponseSubject = new Subject<void>();

//                   const processNextVariable = (index: number) => {
//                     if (index >= variablesWithoutAnnotations.length) {
//                       this.isMultipleVariables = false;
//                       this.userResponseSubject = undefined;
//                       return;
//                     }

//                     const currVariable = variablesWithoutAnnotations[index];
//                     const variableCode = currVariable.name;
//                     const variableLineNumber = currVariable.startLine;

//                     if (lastLine !== undefined && lastLine === variableLineNumber) {
//                       offset += this.currentSuggestion.length;
//                     } else {
//                       offset = 0;
//                     }

//                     const variableRange = new monaco.Range(
//                       currVariable.startLine,
//                       currVariable.startColumn + offset,
//                       currVariable.endLine,
//                       currVariable.endColumn + offset
//                     );

//                     const highlight = editor.createDecorationsCollection([
//                       {
//                         range: variableRange,
//                         options: {
//                           hoverMessage: { value: "Argument without Annotation" },
//                           isWholeLine: false,
//                           className: "annotation-highlight",
//                         },
//                       },
//                     ]);

//                     this.handleTypeAnnotation(
//                       variableCode,
//                       variableRange,
//                       ed as monaco.editor.IStandaloneCodeEditor,
//                       variableLineNumber,
//                       allCode
//                     );

//                     lastLine = variableLineNumber;

//                     if (this.userResponseSubject !== undefined) {
//                       const userResponseSubject = this.userResponseSubject;
//                       const subscription = userResponseSubject
//                         .pipe(take(1))
//                         .pipe(takeUntil(this.componentDestroy))
//                         .subscribe(() => {
//                           highlight.clear();
//                           subscription.unsubscribe();
//                           processNextVariable(index + 1);
//                         });
//                     }
//                   };
//                   processNextVariable(0);
//                 });
//             },
//           });
//         },
//       });
//   }

  // private handleTypeAnnotation(
  //   code: string,
  //   range: monaco.Range,
  //   editor: monaco.editor.IStandaloneCodeEditor,
  //   lineNumber: number,
  //   allcode: string
  // ): void {
  //   this.aiAssistantService
  //     .getTypeAnnotations(code, lineNumber, allcode)
  //     .pipe(takeUntil(this.componentDestroy))
  //     .subscribe({
  //       next: (response: TypeAnnotationResponse) => {
  //         const choices = response.choices || [];
  //         if (choices.length > 0 && choices[0].message && choices[0].message.content) {
  //           this.currentSuggestion = choices[0].message.content.trim();
  //           this.currentCode = code;
  //           this.currentRange = range;

  //           const position = editor.getScrolledVisiblePosition(range.getStartPosition());
  //           if (position) {
  //             this.suggestionTop = position.top + 100;
  //             this.suggestionLeft = position.left + 100;
  //           }

  //           this.showAnnotationSuggestion = true;

  //           if (this.annotationSuggestion) {
  //             this.annotationSuggestion.code = this.currentCode;
  //             this.annotationSuggestion.suggestion = this.currentSuggestion;
  //             this.annotationSuggestion.top = this.suggestionTop;
  //             this.annotationSuggestion.left = this.suggestionLeft;
  //           }
  //         } else {
  //           console.error("Error: OpenAI response does not contain valid message content", response);
  //         }
  //       },
  //       error: (error: unknown) => {
  //         console.error("Error fetching type annotations:", error);
  //       },
  //     });
  // }

//   public acceptCurrentAnnotation(): void {
//     if (!this.showAnnotationSuggestion || !this.currentRange || !this.currentSuggestion) {
//       return;
//     }
  
//     if (this.currentRange && this.currentSuggestion) {
//       const selection = new monaco.Selection(
//         this.currentRange.startLineNumber,
//         this.currentRange.startColumn,
//         this.currentRange.endLineNumber,
//         this.currentRange.endColumn
//       );
  
//       // 确保 this.editor 不为 undefined
//       if (this.editor) {
//         this.insertTypeAnnotations(this.editor, selection, this.currentSuggestion);
  
//         if (this.isMultipleVariables && this.userResponseSubject) {
//           this.userResponseSubject.next();
//         }
//       } else {
//         console.error('Editor is not initialized.');
//       }
//     }
//     this.showAnnotationSuggestion = false;
//   }

//   public rejectCurrentAnnotation(): void {
//     this.showAnnotationSuggestion = false;
//     this.currentCode = "";
//     this.currentSuggestion = "";

//     if (this.isMultipleVariables && this.userResponseSubject) {
//       this.userResponseSubject.next();
//     }
//   }

//   private insertTypeAnnotations(
//     editor: monaco.editor.IStandaloneCodeEditor,
//     selection: monaco.Selection,
//     annotations: string
//   ) {
//     const insertPosition = new monaco.Position(selection.endLineNumber, selection.endColumn);
//     const insertOffset = editor.getModel()?.getOffsetAt(insertPosition) || 0;
//     this.code?.insert(insertOffset, annotations);
//   }

//   private initDiffEditor() {
//     if (this.code) {
//       // this.editor = monaco.editor.createDiffEditor(this.editorElement.nativeElement, {
//       //   readOnly: true,
//       //   theme: "vs-dark",
//       //   fontSize: 11,
//       //   automaticLayout: true,
//       // });
//       const currentWorkflowVersionCode = this.workflowActionService
//         .getTempWorkflow()
//         ?.content.operators?.filter(
//           operator =>
//             operator.operatorID ===
//             this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0]
//         )?.[0].operatorProperties.code;
//       // this.editor?.setModel({
//       //   original: monaco.editor.createModel(this.code.toString(), "python"),
//       //   modified: monaco.editor.createModel(currentWorkflowVersionCode, "python"),
//       // });
//     }
//   }
//   onFocus() {
//         this.workflowActionService.getJointGraphWrapper().highlightOperators(this.operatorID);
//       }
// }



import { AfterViewInit, Component, ComponentRef, ElementRef, OnDestroy, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { YText } from "yjs/dist/src/types/YText";
import { MonacoBinding } from "y-monaco";
import { Subject, take } from "rxjs";
import { takeUntil } from "rxjs/operators";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { FormControl } from "@angular/forms";
import { AIAssistantService, TypeAnnotationResponse } from "../../service/ai-assistant/ai-assistant.service";
import { AnnotationSuggestionComponent } from "./annotation-suggestion.component";

import { MonacoEditorLanguageClientWrapper, UserConfig } from 'monaco-editor-wrapper';
import { WebSocketMessageReader, WebSocketMessageWriter, toSocket } from 'vscode-ws-jsonrpc';
import * as monaco from 'monaco-editor';

@UntilDestroy()
@Component({
  selector: "texera-code-editor",
  templateUrl: "code-editor.component.html",
  styleUrls: ["code-editor.component.scss"],
})
export class CodeEditorComponent implements AfterViewInit, OnDestroy {
  @ViewChild("editor", { static: true }) editorElement!: ElementRef;
  @ViewChild("container", { static: true }) containerElement!: ElementRef;
  @ViewChild(AnnotationSuggestionComponent) annotationSuggestion!: AnnotationSuggestionComponent;
  private code?: YText;
  private editor?: monaco.editor.IStandaloneCodeEditor;
  private workflowVersionStreamSubject: Subject<void> = new Subject<void>();
  private operatorID!: string;
  public title: string | undefined;
  public formControl!: FormControl;
  public componentRef: ComponentRef<CodeEditorComponent> | undefined;
  public language: string = "";
  public languageTitle: string = "";

  private monacoBinding?: MonacoBinding;

  private wrapper?: MonacoEditorLanguageClientWrapper;
  

  public showAnnotationSuggestion: boolean = false;
  public currentCode: string = "";
  public currentSuggestion: string = "";
  public currentRange: monaco.Range | undefined;
  public suggestionTop: number = 0;
  public suggestionLeft: number = 0;
  private userResponseSubject?: Subject<void>;
  private isMultipleVariables: boolean = false;
  private componentDestroy = new Subject<void>();

  private generateLanguageTitle(language: string): string {
    return `${language.charAt(0).toUpperCase()}${language.slice(1)} UDF`;
  }

  changeLanguage(newLanguage: string) {
    this.language = newLanguage;
    console.log("change to ", newLanguage);
    const model = this.editor?.getModel();
    if (model) {
      monaco.editor.setModelLanguage(model, newLanguage);
    }
    this.languageTitle = this.generateLanguageTitle(newLanguage);
  }

  constructor(
    private sanitizer: DomSanitizer,
    private workflowActionService: WorkflowActionService,
    private workflowVersionService: WorkflowVersionService,
    public coeditorPresenceService: CoeditorPresenceService,
    private aiAssistantService: AIAssistantService
  ) {
    const currentOperatorId = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    const operatorType = this.workflowActionService.getTexeraGraph().getOperator(currentOperatorId).operatorType;

    if (operatorType === "RUDFSource" || operatorType === "RUDF") {
      this.changeLanguage("r");
    } else if (
      operatorType === "PythonUDFV2" ||
      operatorType === "PythonUDFSourceV2" ||
      operatorType === "DualInputPortsPythonUDFV2"
    ) {
      this.changeLanguage("python");
    } else {
      this.changeLanguage("java");
    }
  }

  async ngAfterViewInit(): Promise<void> {
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", true);
    this.operatorID = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    this.title = this.workflowActionService.getTexeraGraph().getOperator(this.operatorID).customDisplayName;
    const style = localStorage.getItem(this.operatorID);
    if (style) this.containerElement.nativeElement.style.cssText = style;
    this.code = (
      this.workflowActionService
        .getTexeraGraph()
        .getSharedOperatorType(this.operatorID)
        .get("operatorProperties") as YType<Readonly<{ [key: string]: any }>>
    ).get("code") as YText;

    console.log("added this code ", this.code);

    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(takeUntil(this.workflowVersionStreamSubject))
      .subscribe(async (displayParticularVersion: boolean) => {
        if (displayParticularVersion) {
          this.initDiffEditor();
        } else {
          await this.initMonaco();
          this.formControl.statusChanges.pipe(untilDestroyed(this)).subscribe(() => {
            const editorInstance = this.wrapper?.getEditor();
            if (editorInstance) {
              editorInstance.updateOptions({
                readOnly: this.formControl.disabled,
              });
            }
          });
        }
      });
  }

  private handleTypeAnnotation(
    code: string,
    range: monaco.Range,
    editor: monaco.editor.IStandaloneCodeEditor,
    lineNumber: number,
    allcode: string
  ): void {
    this.aiAssistantService
      .getTypeAnnotations(code, lineNumber, allcode)
      .pipe(takeUntil(this.componentDestroy))
      .subscribe({
        next: (response: TypeAnnotationResponse) => {
          const choices = response.choices || [];
          if (choices.length > 0 && choices[0].message && choices[0].message.content) {
            this.currentSuggestion = choices[0].message.content.trim();
            this.currentCode = code;
            this.currentRange = range;

            const position = editor.getScrolledVisiblePosition(range.getStartPosition());
            if (position) {
              this.suggestionTop = position.top + 100;
              this.suggestionLeft = position.left + 100;
            }

            this.showAnnotationSuggestion = true;

            if (this.annotationSuggestion) {
              this.annotationSuggestion.code = this.currentCode;
              this.annotationSuggestion.suggestion = this.currentSuggestion;
              this.annotationSuggestion.top = this.suggestionTop;
              this.annotationSuggestion.left = this.suggestionLeft;
            }
          } else {
            console.error("Error: OpenAI response does not contain valid message content", response);
          }
        },
        error: (error: unknown) => {
          console.error("Error fetching type annotations:", error);
        },
      });
  }

  public getCoeditorCursorStyles(coeditor: Coeditor) {
    const textCSS =
      "<style>" +
      `.yRemoteSelection-${coeditor.clientId} { background-color: ${coeditor.color?.replace("0.8", "0.5")}}` +
      `.yRemoteSelectionHead-${coeditor.clientId}::after { border-color: ${coeditor.color}}` +
      `.yRemoteSelectionHead-${coeditor.clientId} { border-color: ${coeditor.color}}` +
      "</style>";
    return this.sanitizer.bypassSecurityTrustHtml(textCSS);
  }

  async ngOnDestroy(): Promise<void> {
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", false);
    localStorage.setItem(this.operatorID, this.containerElement.nativeElement.style.cssText);



    if (this.monacoBinding) {
      this.monacoBinding.destroy();
    }
  
    if (this.editor) {
      this.editor.dispose();
    }
  
    if (this.wrapper) {
      await this.wrapper.dispose(true);
    }
    

    if (this.workflowVersionStreamSubject) {
      this.workflowVersionStreamSubject.next();
      this.workflowVersionStreamSubject.complete();
    }

    this.componentDestroy.next();
    this.componentDestroy.complete();
   

  }

  private async initMonaco() {
    if (this.wrapper !== undefined) {
      this.wrapper.dispose();
    }

    if (!this.wrapper) {
      this.wrapper = new MonacoEditorLanguageClientWrapper();

      const userConfig: UserConfig = {
        wrapperConfig: {
          editorAppConfig: {
            $type: 'extended',
            codeResources: {
              main: {
                text: this.code ? this.code.toString() : 'print("Hello, World!")',
                uri: `in-memory-${this.operatorID}.py`
              }
            },
            userConfiguration: {
              json: JSON.stringify({
                'workbench.colorTheme': 'Default Dark Modern',
              })
            }
          }
        },
        languageClientConfig: {
          languageId: this.language || 'python',
          options: {
            $type: 'WebSocketUrl',
            url: 'ws://localhost:3000/language-server',
            startOptions: {
              onCall: () => {
                console.log('Language client started');
              },
              reportStatus: true,
            }
          }
        }
      };

      try {
        await this.wrapper.initAndStart(userConfig, this.editorElement.nativeElement);

        this.formControl.statusChanges.subscribe(() => {
          const editorInstance = this.wrapper?.getEditor();
          if (editorInstance) {
            editorInstance.updateOptions({
              readOnly: this.formControl.disabled,
            });
          }
        });

      } catch (e) {
        console.error('Error during Monaco Editor initialization:', e);
      }
    }

    this.editor = this.wrapper?.getEditor();

    // if (this.code && editor) {
    //   new MonacoBinding(
    //     this.code,
    //     editor.getModel()!,
    //     new Set([editor]),
    //     this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
    //   );
    // }

    //有用
    if (this.code && this.editor) {
      // 如果已经有旧的 binding，先 dispose
      if (this.monacoBinding) {
        this.monacoBinding.destroy();  // 假设 destroy 是同步方法
        this.monacoBinding = undefined;
      }
      
    
      // 创建新的 MonacoBinding
      this.monacoBinding = new MonacoBinding(
        this.code,
        this.editor.getModel()!,
        new Set([this.editor]),
        this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
      );
    }

    // Check if the AI provider is "openai"
    this.aiAssistantService
      .checkAIAssistantEnabled()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (isEnabled: string) => {
          if (isEnabled === "OpenAI") {
            this.editor?.addAction({
              id: "type-annotation-action",
              label: "Add Type Annotation",
              contextMenuGroupId: "1_modification",
              contextMenuOrder: 1.0,
              run: ed => {
                const selection = ed.getSelection();
                const model = ed.getModel();
                if (!model || !selection) {
                  return;
                }
                const code = model.getValueInRange(selection);
                const lineNumber = selection.startLineNumber;
                const allcode = model.getValue();
                this.handleTypeAnnotation(
                  code,
                  selection,
                  ed as monaco.editor.IStandaloneCodeEditor,
                  lineNumber,
                  allcode
                );
              },
            });
          }

          this.editor?.addAction({
            id: "all-type-annotation-action",
            label: "Add All Type Annotations",
            contextMenuGroupId: "1_modification",
            contextMenuOrder: 1.1,
            run: ed => {
              const selection = ed.getSelection();
              const model = ed.getModel();
              if (!model || !selection) {
                return;
              }

              const selectedCode = model.getValueInRange(selection);
              const allCode = model.getValue();

              this.aiAssistantService
                .locateUnannotated(selectedCode, selection.startLineNumber)
                .pipe(takeUntil(this.componentDestroy))
                .subscribe(variablesWithoutAnnotations => {
                  if (variablesWithoutAnnotations.length === 0) {
                    return;
                  }

                  let offset = 0;
                  let lastLine: number | undefined;

                  this.isMultipleVariables = true;
                  this.userResponseSubject = new Subject<void>();

                  const processNextVariable = (index: number) => {
                    if (index >= variablesWithoutAnnotations.length) {
                      this.isMultipleVariables = false;
                      this.userResponseSubject = undefined;
                      return;
                    }

                    const currVariable = variablesWithoutAnnotations[index];
                    const variableCode = currVariable.name;
                    const variableLineNumber = currVariable.startLine;

                    if (lastLine !== undefined && lastLine === variableLineNumber) {
                      offset += this.currentSuggestion.length;
                    } else {
                      offset = 0;
                    }

                    const variableRange = new monaco.Range(
                      currVariable.startLine,
                      currVariable.startColumn + offset,
                      currVariable.endLine,
                      currVariable.endColumn + offset
                    );

                    const highlight = this.editor?.createDecorationsCollection([
                      {
                        range: variableRange,
                        options: {
                          hoverMessage: { value: "Argument without Annotation" },
                          isWholeLine: false,
                          className: "annotation-highlight",
                        },
                      },
                    ]);

                    this.handleTypeAnnotation(
                      variableCode,
                      variableRange,
                      ed as monaco.editor.IStandaloneCodeEditor,
                      variableLineNumber,
                      allCode
                    );

                    lastLine = variableLineNumber;

                    if (this.userResponseSubject !== undefined) {
                      const userResponseSubject = this.userResponseSubject;
                      const subscription = userResponseSubject
                        .pipe(take(1))
                        .pipe(takeUntil(this.componentDestroy))
                        .subscribe(() => {
                          highlight?.clear();
                          subscription.unsubscribe();
                          processNextVariable(index + 1);
                        });
                    }
                  };
                  processNextVariable(0);
                });
            },
          });
        },
      });
  }

  public acceptCurrentAnnotation(): void {
    if (!this.showAnnotationSuggestion || !this.currentRange || !this.currentSuggestion) {
      return;
    }
  
    if (this.currentRange && this.currentSuggestion) {
      const selection = new monaco.Selection(
        this.currentRange.startLineNumber,
        this.currentRange.startColumn,
        this.currentRange.endLineNumber,
        this.currentRange.endColumn
      );
  
      if (this.editor) {
        this.insertTypeAnnotations(this.editor, selection, this.currentSuggestion);
  
        if (this.isMultipleVariables && this.userResponseSubject) {
          this.userResponseSubject.next();
        }
      } else {
        console.error('Editor is not initialized.');
      }
    }
    this.showAnnotationSuggestion = false;
  }

  public rejectCurrentAnnotation(): void {
    this.showAnnotationSuggestion = false;
    this.currentCode = "";
    this.currentSuggestion = "";

    if (this.isMultipleVariables && this.userResponseSubject) {
      this.userResponseSubject.next();
    }
  }

  private insertTypeAnnotations(
    editor: monaco.editor.IStandaloneCodeEditor,
    selection: monaco.Selection,
    annotations: string
  ) {
    const insertPosition = new monaco.Position(selection.endLineNumber, selection.endColumn);
    const insertOffset = editor.getModel()?.getOffsetAt(insertPosition) || 0;
    this.code?.insert(insertOffset, annotations);
  }

  private async initDiffEditor(): Promise<void> {
    if (this.wrapper) {
      await this.wrapper.dispose(true);
    }
    
    if (this.code && !this.wrapper) {
      // 如果已经有旧的绑定，先销毁
  
      this.wrapper = new MonacoEditorLanguageClientWrapper();
  
      const currentWorkflowVersionCode = this.workflowActionService
        .getTempWorkflow()
        ?.content.operators?.filter(
          operator =>
            operator.operatorID ===
            this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0]
        )?.[0].operatorProperties.code;
  
      const userConfig: UserConfig = {
        wrapperConfig: {
          editorAppConfig: {
            $type: 'extended',  // 如果支持 diff 编辑器
            codeResources: {
              main: {
                text: this.code.toString(),
                uri: `in-memory-${this.operatorID}.py`
                
              },
              original: {
                text: currentWorkflowVersionCode,
                uri: `in-memory-${this.operatorID}-version.py`
              }
            },
            useDiffEditor: true,
            userConfiguration: {
              json: JSON.stringify({
                'workbench.colorTheme': 'Default Dark Modern',
              })
            }
          }
        },
        languageClientConfig: {
          languageId: this.language || 'python',  // 例如使用 'python' 语言
          options: {
            $type: 'WebSocketUrl',
            url: 'ws://localhost:3000/language-server',
            startOptions: {
              onCall: () => {
                console.log('Language client started');
              },
              reportStatus: true,
            }
          }
        }
      };
  
      try {
        // 使用 wrapper 初始化并开始 diff 编辑器
        await this.wrapper.initAndStart(userConfig, this.editorElement.nativeElement);
      } catch (e) {
        console.error('Error during Monaco Editor initialization:', e);
      }
  
      // 检查是否初始化完成
      // const diffEditor = this.wrapper?.getDiffEditor();
  
      // if (this.code && diffEditor) {
      //   diffEditor.setModel({
      //     original: monaco.editor.createModel(currentWorkflowVersionCode || '', 'python'),
      //     modified: monaco.editor.createModel(this.code.toString(), 'python'),
      //   });
      // }
    }
  }
  

  onFocus() {
    this.workflowActionService.getJointGraphWrapper().highlightOperators(this.operatorID);
  }
}

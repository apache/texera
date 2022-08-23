import {AfterViewInit, Component, ElementRef, Inject, OnDestroy, OnInit, ViewChild} from "@angular/core";
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowCollabService } from "../../service/workflow-collab/workflow-collab.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import {OperatorPredicate, YType} from "../../types/workflow-common.interface";
import {YText} from "yjs/dist/src/types/YText";
import {MonacoBinding} from "y-monaco";
import {Subject} from "rxjs";
import {first} from "rxjs/operators";
import {CoeditorPresenceService} from "../../service/workflow-graph/model/coeditor-presence.service";
import {DomSanitizer, SafeStyle} from "@angular/platform-browser";
import {User} from "../../../common/type/user";
declare const monaco: any;

/**
 * CodeEditorDialogComponent is the content of the dialogue invoked by CodeareaCustomTemplateComponent.
 *
 * It contains a Monaco editor which is inside a mat-dialog-content. When the dialogue is invoked by
 * the button in CodeareaCustomTemplateComponent, the data of the custom field (or empty String if no data)
 * will be sent to the Monaco editor as its text. The dialogue can be closed with ESC key or by clicking on areas outside
 * the dialogue. Closing the dialogue will send the edited contend back to the custom template field.
 */
@UntilDestroy()
@Component({
  selector: "texera-code-editor-dialog",
  templateUrl: "./code-editor-dialog.component.html",
  styleUrls: ["./code-editor-dialog.component.scss"],
})
export class CodeEditorDialogComponent implements AfterViewInit, SafeStyle, OnDestroy{
  editorOptions = {
    theme: "vs-dark",
    language: "python",
    fontSize: "11",
    automaticLayout: true,
    readOnly: true,
  };
  code: string;
  @ViewChild("editor", { static: true }) divEditor: ElementRef | undefined;
  loaded: boolean = false;

  public loadingFinished: Subject<void> = new Subject<void>();
  public lockGranted: boolean = false;
  private ytext?: YText;

  constructor(
    private sanitizer: DomSanitizer,
    private dialogRef: MatDialogRef<CodeEditorDialogComponent>,
    @Inject(MAT_DIALOG_DATA) code: any,
    private workflowActionService: WorkflowActionService,
    private workflowCollabService: WorkflowCollabService,
    public coeditorPresenceService: CoeditorPresenceService
  ) {
    this.code = code;
    this.handleLockChange();
  }

  ngOnDestroy(): void {
      this.workflowActionService.getTexeraGraph().sharedModel.updateAwareness("editingCode", false);
    }

  private finishLoading() {
    this.loaded = true;
    this.loadingFinished.next();
  }

  public load() {
    // load the assets

    const baseUrl = "./assets" + "/monaco-editor/min/vs";

    if (typeof (<any>window).monaco === "object") {
      this.finishLoading();
      return;
    }

    const onGotAmdLoader: any = () => {
      // load Monaco
      (<any>window).require.config({ paths: { vs: `${baseUrl}` } });
      (<any>window).require(["vs/editor/editor.main"], () => {
        this.finishLoading();
      });
    };

    // load AMD loader, if necessary
    if (!(<any>window).require) {
      const loaderScript: HTMLScriptElement = document.createElement("script");
      loaderScript.type = "text/javascript";
      loaderScript.src = `${baseUrl}/loader.js`;
      loaderScript.addEventListener("load", onGotAmdLoader);
      document.body.appendChild(loaderScript);
    } else {
      onGotAmdLoader();
    }
  }

  ngAfterViewInit() {
    const currentOperatorId: string = this.workflowActionService
    .getJointGraphWrapper()
    .getCurrentHighlightedOperatorIDs()[0];

    this.workflowActionService.getTexeraGraph().sharedModel.updateAwareness("editingCode", true);

    this.ytext = ((this.workflowActionService.getTexeraGraph().sharedModel.operatorIDMap.get(currentOperatorId) as YType<OperatorPredicate>).get("operatorProperties") as YType<Readonly<{ [key: string]: any }>>).get("code") as YText;

    this.load();
    this.initMonaco();
  }

  private initMonaco(){
    if (!this.loaded) {
      // eslint-disable-next-line rxjs-angular/prefer-takeuntil
      this.loadingFinished.pipe(first()).subscribe(()=>{
        this.initMonaco();
      });
      return;
    }
    const editor = monaco.editor.create(this.divEditor?.nativeElement, this.editorOptions);
    if (this.ytext)
      new MonacoBinding(this.ytext, editor.getModel(), new Set([editor]), this.workflowActionService.getTexeraGraph().sharedModel.awareness);
  }

  private handleLockChange(): void {
    this.workflowCollabService
      .getLockStatusStream()
      .pipe(untilDestroyed(this))
      .subscribe((lockGranted: boolean) => {
        this.lockGranted = lockGranted;
        this.editorOptions.readOnly = !this.lockGranted;
      });
  }

  public getCoeditorCursorStyles(coeditor: User) {
    const textCSS = "<style>" + `.yRemoteSelection-${coeditor.clientId} { background-color: ${coeditor.color?.replace("0.8", "0.5")}}` +
      `.yRemoteSelectionHead-${coeditor.clientId}::after { border-color: ${coeditor.color}}` +
      `.yRemoteSelectionHead-${coeditor.clientId} { border-color: ${coeditor.color}}` + "</style>";
    return this.sanitizer.bypassSecurityTrustHtml(textCSS);
  }
}

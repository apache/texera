import { AfterViewInit, Component, ElementRef, Inject, OnDestroy, ViewChild } from "@angular/core";
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { YText } from "yjs/dist/src/types/YText";
import { MonacoBinding } from "y-monaco";
import { MonacoLanguageClient, CloseAction, ErrorAction, MessageTransports } from "monaco-languageclient";
import { toSocket, WebSocketMessageReader, WebSocketMessageWriter } from "vscode-ws-jsonrpc";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { FormControl } from "@angular/forms";
import { getWebsocketUrl } from "src/app/common/util/url";

declare const monaco: any;

/**
 * CodeEditorDialogComponent is the content of the dialogue invoked by CodeareaCustomTemplateComponent.
 *
 * It contains a shared-editable Monaco editor which is inside a mat-dialog-content. When the dialogue is invoked by
 * the button in CodeareaCustomTemplateComponent, this component will use the actual y-text of the code within the
 * operator property to connect to the editor.
 *
 * The original Monaco Editor is used here instead of ngx-monaco-editor to accommodate MonacoBinding.
 *
 * The dialogue can be closed with ESC key or by clicking on areas outside
 * the dialogue. Closing the dialogue will send the edited contend back to the custom template field.
 */
@UntilDestroy()
@Component({
  selector: "texera-code-editor-dialog",
  templateUrl: "./code-editor-dialog.component.html",
  styleUrls: ["./code-editor-dialog.component.scss"],
})
export class CodeEditorDialogComponent implements AfterViewInit, SafeStyle, OnDestroy {
  editorOptions = {
    theme: "vs-dark",
    language: "python",
    fontSize: "11",
    automaticLayout: true,
  };
  @ViewChild("editor", { static: true }) divEditor?: ElementRef;
  private formControl: FormControl;
  private code?: YText;
  private editor?: any;
  private languageServerSocket?: WebSocket;

  constructor(
    private sanitizer: DomSanitizer,
    private dialogRef: MatDialogRef<CodeEditorDialogComponent>,
    @Inject(MAT_DIALOG_DATA) formControl: FormControl,
    private workflowActionService: WorkflowActionService,
    public coeditorPresenceService: CoeditorPresenceService
  ) {
    this.formControl = formControl;
  }

  ngOnDestroy(): void {
    const dialog = document.getElementById("mat-dialog-udf");
    if (dialog !== null) {
      localStorage.setItem("udfDialogStyle", dialog.style.cssText);
    }

    const panel = document.querySelector<HTMLElement>(".cdk-overlay-pane");
    if (panel !== null) {
      localStorage.setItem("udfPanelStyle", panel.style.cssText);
    }

    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", false);

    if (
      this.languageServerSocket !== undefined &&
      this.languageServerSocket.readyState === this.languageServerSocket.OPEN
    ) {
      this.languageServerSocket.close();
      this.languageServerSocket = undefined;
    }

    if (this.editor !== undefined) {
      this.editor.dispose();
    }
  }

  createLanguageClient(transports: MessageTransports): MonacoLanguageClient {
    return new MonacoLanguageClient({
      name: "Python UDF Language Client",
      clientOptions: {
        documentSelector: ["python"],
        errorHandler: {
          error: () => ({ action: ErrorAction.Continue }),
          closed: () => ({ action: CloseAction.Restart }),
        },
      },
      connectionProvider: {
        get: () => {
          return Promise.resolve(transports);
        },
      },
    });
  }

  getLanguageServerSocket() {
    return this.languageServerSocket;
  }

  ngAfterViewInit() {
    const dialog = document.getElementById("mat-dialog-udf");
    if (dialog !== null) {
      // They are the actual width and height of the modal, the width and height in css will be the max size of resizing.
      dialog.style.width = "800px";
      dialog.style.height = "600px";
    }
    const dialogStyle = localStorage.getItem("udfDialogStyle");
    if (dialog !== null && dialogStyle !== null) {
      dialog.style.cssText = dialogStyle;
    }

    const panel = document.querySelector<HTMLElement>(".cdk-overlay-pane");
    const panelStyle = localStorage.getItem("udfPanelStyle");
    if (panel !== null && panelStyle !== null) {
      panel.style.cssText = panelStyle;
    }

    const currentOperatorId: string = this.workflowActionService
      .getJointGraphWrapper()
      .getCurrentHighlightedOperatorIDs()[0];

    if (currentOperatorId === undefined) {
      return;
    }

    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", true);

    this.code = (
      this.workflowActionService
        .getTexeraGraph()
        .getSharedOperatorType(currentOperatorId)
        .get("operatorProperties") as YType<Readonly<{ [key: string]: any }>>
    ).get("code") as YText;

    this.initMonaco();
    this.handleDisabledStatusChange();
  }

  /**
   * Specify the co-editor's cursor style. This step is missing from MonacoBinding.
   * @param coeditor
   */
  public getCoeditorCursorStyles(coeditor: Coeditor) {
    const textCSS =
      "<style>" +
      `.yRemoteSelection-${coeditor.clientId} { background-color: ${coeditor.color?.replace("0.8", "0.5")}}` +
      `.yRemoteSelectionHead-${coeditor.clientId}::after { border-color: ${coeditor.color}}` +
      `.yRemoteSelectionHead-${coeditor.clientId} { border-color: ${coeditor.color}}` +
      "</style>";
    return this.sanitizer.bypassSecurityTrustHtml(textCSS);
  }

  /**
   * Create a Monaco editor and connect it to MonacoBinding.
   * @private
   */
  private initMonaco() {
    const editor = monaco.editor.create(this.divEditor?.nativeElement, this.editorOptions);
    if (this.code) {
      new MonacoBinding(
        this.code,
        editor.getModel(),
        new Set([editor]),
        this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
      );
    }
    this.editor = editor;
    this.connectLanguageServer();
  }

  /**
   * Create a Monaco editor and connect it to MonacoBinding.
   * @private
   */
  private connectLanguageServer() {
    const url = getWebsocketUrl("/python-language-server", "3000");

    if (this.languageServerSocket === undefined) {
      this.languageServerSocket = new WebSocket(url);
      this.languageServerSocket.onopen = () => {
        if (this.languageServerSocket !== undefined) {
          const socket = toSocket(this.languageServerSocket);
          const reader = new WebSocketMessageReader(socket);
          const writer = new WebSocketMessageWriter(socket);
          const languageClient = this.createLanguageClient({
            reader,
            writer,
          });
          languageClient.start();
          reader.onClose(() => languageClient.stop());
        }
      };
    }
  }

  /**
   * Uses the formControl's status to change readonly status of the editor.
   * @private
   */
  private handleDisabledStatusChange(): void {
    this.formControl.statusChanges.pipe(untilDestroyed(this)).subscribe(_ => {
      this.editor.updateOptions({
        readOnly: this.formControl.disabled,
      });
    });
  }
}

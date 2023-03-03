import { AfterViewInit, Component, ElementRef, Inject, OnDestroy, ViewChild } from "@angular/core";
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { YText } from "yjs/dist/src/types/YText";
import { MonacoBinding } from "y-monaco";
import {
  MonacoLanguageClient,
  CloseAction,
  ErrorAction,
  MessageTransports,
  MonacoServices,
} from "monaco-languageclient";
import { toSocket, WebSocketMessageReader, WebSocketMessageWriter } from "vscode-ws-jsonrpc";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { FormControl } from "@angular/forms";
import { createUrl } from "../../../common/util/url";

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
    model: monaco.editor.createModel(this.code, "python", monaco.Uri.parse(MODEL_URI)),
    theme: "vs-dark",
    language: "python",
    fontSize: "11",
    automaticLayout: true,
  };
  @ViewChild("editor", { static: true }) divEditor: ElementRef | undefined;
  loaded: boolean = false;

  private formControl: FormControl;
  private code?: YText;
  private editor?: any;

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
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", false);
  }

  createLanguageClient(transports: MessageTransports): MonacoLanguageClient {
    return new MonacoLanguageClient({
      name: "Python UDF Language Client",
      clientOptions: {
        documentSelector: ["python"],
        errorHandler: {
          error: () => ({ action: ErrorAction.Continue }),
          closed: () => ({ action: CloseAction.DoNotRestart }),
        },
      },
      connectionProvider: {
        get: () => {
          return Promise.resolve(transports);
        },
      },
    });
  }

  ngAfterViewInit() {
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

    MonacoServices.install();

    const url = createUrl(WEB_SOCKET_HOST, LANGUAGE_SERVER_PORT, PYTHON_LANGUAGE_SERVER);
    const webSocket = new WebSocket(url);

    webSocket.onopen = () => {
      const socket = toSocket(webSocket);
      const reader = new WebSocketMessageReader(socket);
      const writer = new WebSocketMessageWriter(socket);
      const languageClient = this.createLanguageClient({
        reader,
        writer,
      });
      languageClient.start();
      reader.onClose(() => languageClient.stop());
    };
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

/**
 * should refactor this, as this is not the right place to create a websocket connection.
 * putting this now, so the new features of enhanced-python-udf can branch off from this
 * one and start working parallely.
 *  */
const WEB_SOCKET_HOST = "localhost";
const PYTHON_LANGUAGE_SERVER = "/python-language-server";
const LANGUAGE_SERVER_PORT = 3000;
const MODEL_URI = "inmemory://model.py";

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component, ElementRef, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { ConsoleMessage } from "../../../types/workflow-common.interface";
import { ExecutionState } from "src/app/workspace/types/execute-workflow.interface";
import {
  OperatorConsoleMessage,
  WorkflowConsoleService,
} from "../../../service/workflow-console/workflow-console.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CdkVirtualScrollViewport } from "@angular/cdk/scrolling";
import { presetPalettes } from "@ant-design/colors";
import { isDefined } from "../../../../common/util/predicate";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UdfDebugService } from "../../../service/operator-debug/udf-debug.service";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzMenuDirective, NzMenuDividerDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { FormsModule } from "@angular/forms";
import { NzListComponent, NzListItemComponent } from "ng-zorro-antd/list";
import { NgFor, NgIf, DatePipe } from "@angular/common";
import { NzRowDirective, NzColDirective } from "ng-zorro-antd/grid";
import { NzBadgeComponent } from "ng-zorro-antd/badge";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputGroupComponent, NzInputDirective } from "ng-zorro-antd/input";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";

@UntilDestroy()
@Component({
  selector: "texera-console-frame",
  templateUrl: "./console-frame.component.html",
  styleUrls: ["./console-frame.component.scss"],
  imports: [
    NzDropdownDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NzMenuItemComponent,
    NzMenuDividerDirective,
    NzCheckboxComponent,
    NzSwitchComponent,
    FormsModule,
    NzListComponent,
    NgFor,
    NzListItemComponent,
    NzRowDirective,
    NzColDirective,
    NzBadgeComponent,
    NgIf,
    NzCollapseComponent,
    NzCollapsePanelComponent,
    NzTagComponent,
    NzSpaceCompactItemDirective,
    NzInputGroupComponent,
    NzButtonComponent,
    NzWaveDirective,
    NzTooltipDirective,
    NzInputDirective,
    NzSelectComponent,
    NzOptionComponent,
    DatePipe,
  ],
})
export class ConsoleFrameComponent implements OnInit, OnChanges {
  // The operator whose messages are shown. Left out, every operator's messages are shown together.
  @Input() operatorId?: string;
  @Input() consoleInputEnabled?: boolean;
  @ViewChild(CdkVirtualScrollViewport) viewPort?: CdkVirtualScrollViewport;
  @ViewChild("consoleList", { read: ElementRef }) listElement?: ElementRef;

  // The template renders `filteredMessages`, recomputed whenever the messages
  // or the filter change, so change detection never re-filters the list.
  private rawConsoleMessages: ReadonlyArray<ConsoleMessage> = [];
  filteredMessages: ReadonlyArray<ConsoleMessage> = [];

  get consoleMessages(): ReadonlyArray<ConsoleMessage> {
    return this.rawConsoleMessages;
  }

  set consoleMessages(messages: ReadonlyArray<ConsoleMessage>) {
    this.rawConsoleMessages = messages;
    this.applyTypeFilter();
  }

  // The operator each shown message came from, for its tag when every operator's messages are shown.
  private messageOperator = new Map<ConsoleMessage, string>();
  // the console message made for each error that stopped the run or the compilation
  private fatalErrorRows = new WeakMap<WorkflowFatalError, OperatorConsoleMessage>();

  // Message types the user has switched off. Absent from the set means visible,
  // so a type the console has never seen before is shown rather than hidden.
  private hiddenTypes = new Set<string>();

  // Mirrors each dropdown's open state so the trigger can expose aria-expanded,
  // which nz-dropdown does not set itself.
  settingsMenuOpen = false;
  typeFilterMenuOpen = false;

  // Configuration Menu items
  // TODO: move Configuration Menu to a separate component
  showTimestamp: boolean = true;
  showSource: boolean = true;

  // WorkerId Menu related items.
  ALL_WORKERS: string = "All Workers";
  workerIds: readonly string[] = [];
  command: string = "";
  targetWorker: string = this.ALL_WORKERS;

  labelMapping = new Map([
    ["PRINT", "default"],
    ["COMMAND", "processing"],
    ["DEBUGGER", "warning"],
    ["ERROR", "error"],
  ]);

  // The order here drives the order of the checkboxes in the filter menu.
  readonly messageTypes: ReadonlyArray<string> = Array.from(this.labelMapping.keys());

  isTypeVisible(type: string): boolean {
    return !this.hiddenTypes.has(type);
  }

  setTypeVisibility(type: string, visible: boolean): void {
    if (visible) {
      this.hiddenTypes.delete(type);
    } else {
      this.hiddenTypes.add(type);
    }
    this.applyTypeFilter();
  }

  typeLabel(type: string): string {
    return type.charAt(0) + type.slice(1).toLowerCase();
  }

  get allTypesVisible(): boolean {
    return this.messageTypes.every(type => this.isTypeVisible(type));
  }

  get typeFilterIndeterminate(): boolean {
    return !this.allTypesVisible && this.messageTypes.some(type => this.isTypeVisible(type));
  }

  setAllTypesVisible(visible: boolean): void {
    this.hiddenTypes = visible ? new Set<string>() : new Set<string>(this.messageTypes);
    this.applyTypeFilter();
  }

  get hiddenMessageCount(): number {
    return this.rawConsoleMessages.length - this.filteredMessages.length;
  }

  private applyTypeFilter(): void {
    this.filteredMessages =
      this.hiddenTypes.size === 0
        ? this.rawConsoleMessages
        : this.rawConsoleMessages.filter(message => this.isTypeVisible(message.msgType.name));
  }

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowConsoleService: WorkflowConsoleService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private notificationService: NotificationService,
    private udfDebugService: UdfDebugService,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.operatorId) this.operatorId = changes.operatorId.currentValue;
    this.renderConsole();
  }

  ngOnInit(): void {
    // make sure the console is re-rendered upon state changes
    this.registerAutoConsoleRerender();
  }

  registerAutoConsoleRerender(): void {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (event.previous.state === ExecutionState.Initializing && event.current.state === ExecutionState.Running) {
          // clear the console for the next execution
          this.clearConsole();
        } else {
          // re-render the console, this may update the console with error messages or console messages
          this.renderConsole();
        }
      });

    this.workflowConsoleService
      .getConsoleMessageUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(_ => this.renderConsole());

    // a compilation error is shown here too, and it comes and goes without a run
    this.workflowCompilingService
      .getCompilationStateInfoChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(_ => this.renderConsole());
  }

  clearConsole(): void {
    this.consoleMessages = [];
  }

  renderConsole(): void {
    this.workerIds = this.operatorId ? this.executeWorkflowService.getWorkerIds(this.operatorId) : [];
    this.displayConsoleMessages(this.operatorId);
  }

  /**
   * Show the operator's messages, or every operator's when none is given. The errors that stopped
   * the run or the compilation come last, as ERROR messages.
   */
  displayConsoleMessages(operatorId?: string): void {
    const lastVisible = this.filteredMessages[this.filteredMessages.length - 1];
    const rows: OperatorConsoleMessage[] = isDefined(operatorId)
      ? (this.workflowConsoleService.getConsoleMessages(operatorId) ?? []).map(message => ({ operatorId, message }))
      : [...this.workflowConsoleService.getAllConsoleMessages()];
    rows.push(...this.fatalErrorMessages().filter(row => !isDefined(operatorId) || row.operatorId === operatorId));
    this.messageOperator = new Map(rows.map(row => [row.message, row.operatorId]));
    this.consoleMessages = rows.map(row => row.message);

    // Preserve scroll position when no new visible message arrives.
    if (this.filteredMessages[this.filteredMessages.length - 1] === lastVisible) {
      return;
    }
    setTimeout(() => {
      if (this.listElement) {
        this.listElement.nativeElement.scrollTop = this.listElement.nativeElement.scrollHeight;
      }
    }, 0);
  }

  submitDebugCommand(): void {
    if (!isDefined(this.operatorId)) {
      return;
    }
    let workers = [];
    if (this.targetWorker === this.ALL_WORKERS) {
      workers = [...this.workerIds];
    } else {
      workers.push(this.targetWorker);
    }
    for (let worker of workers) {
      this.workflowWebsocketService.send("DebugCommandRequest", {
        operatorId: this.operatorId,
        workerId: worker,
        cmd: this.command,
      });
    }
    this.command = "";
  }

  workerIdToAbbr(workerId: string): string {
    return "W" + this.getWorkerIndex(workerId);
  }

  getWorkerColor(workerIndex: number): string {
    const presetPalettesSize = Object.keys(presetPalettes).length;

    // exclude red (index 0) and volcano (index 1) as they look as warning/error.
    // use *3 to diff colors between adjacent workers.
    const colorKey = Object.keys(presetPalettes)[2 + ((workerIndex * 3) % (presetPalettesSize - 2))];

    // use shade index >=6 as they are dark enough.
    return presetPalettes[colorKey][
      6 + ((Math.floor(workerIndex / presetPalettesSize) * 3) % (presetPalettes[colorKey].length - 6))
    ];
  }

  getWorkerIndex(workerId: string): number {
    const tokens = workerId.split("-");
    return parseInt(tokens.at(tokens.length - 1) || "0");
  }

  onClickSkipTuples(): void {
    try {
      this.executeWorkflowService.skipTuples(this.workerIds);
    } catch (e) {
      this.notificationService.error((e as Error).message);
    }
  }

  onClickRetryTuples() {
    try {
      this.executeWorkflowService.retryExecution(this.workerIds);
    } catch (e) {
      this.notificationService.error((e as Error).message);
    }
  }

  onClickStep(): void {
    if (!isDefined(this.operatorId)) return;
    for (let worker of this.workerIds) {
      this.udfDebugService.doStep(this.operatorId, worker);
    }
  }

  onClickContinue(): void {
    if (!isDefined(this.operatorId)) return;
    for (let worker of this.workerIds) {
      this.udfDebugService.doContinue(this.operatorId, worker);
    }
  }

  getMessageLabel(message: ConsoleMessage): string {
    return this.labelMapping.get(message.msgType.name) ?? "";
  }

  /** The operator a shown message came from, when it came from one still on the canvas. */
  messageOperatorId(message: ConsoleMessage): string | undefined {
    const operatorId = this.messageOperator.get(message);
    return isDefined(operatorId) && this.workflowActionService.getTexeraGraph().hasOperator(operatorId)
      ? operatorId
      : undefined;
  }

  operatorLabel(operatorId: string): string {
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasOperator(operatorId)) return operatorId;
    const operator = graph.getOperator(operatorId);
    return operator.customDisplayName ?? operator.operatorType;
  }

  /** Show only this operator's messages. The panel follows the highlight, so this highlights it. */
  showOperator(operatorId: string): void {
    this.workflowActionService.highlightOperators(false, operatorId);
  }

  /** Back to every operator's messages, by clearing the highlight the panel follows. */
  showAllOperators(): void {
    const wrapper = this.workflowActionService.getJointGraphWrapper();
    wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
  }

  /** The errors that stopped the run or the compilation, as console messages. */
  private fatalErrorMessages(): OperatorConsoleMessage[] {
    const errors: WorkflowFatalError[] = [
      ...this.executeWorkflowService.getErrorMessages(),
      ...Object.values(this.workflowCompilingService.getWorkflowCompilationErrors()),
    ];
    return errors.map(error => {
      // The same error gives the same message on every render. A new one each time would read as
      // a message just arrived, and the list would jump to its end while the user scrolls up.
      let row = this.fatalErrorRows.get(error);
      if (!row) {
        const shouldFormat = error.type.name === "COMPILATION_ERROR" || error.type.name === "EXECUTION_FAILURE";
        row = {
          operatorId: error.operatorId,
          message: {
            workerId: error.workerId,
            timestamp: error.timestamp,
            msgType: { name: "ERROR" },
            source: error.type.name,
            title: shouldFormat ? readableError(error.message) : error.message,
            message: shouldFormat ? readableError(error.details) : error.details,
          },
        };
        this.fatalErrorRows.set(error, row);
      }
      return row;
    });
  }
}

/** Strip a Java exception's class name and a failed requirement's prefix, which mean nothing to a user. */
function readableError(text: string): string {
  if (!text) return text;
  return text
    .replace(/^\s*(?:(?:[a-zA-Z0-9_]+\.)*[a-zA-Z0-9_]+(?:Exception|Error)):\s*/, "")
    .replace(/^\s*requirement failed:\s*/, "");
}

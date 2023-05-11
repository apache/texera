import { Component, ElementRef, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { BreakpointTriggerInfo, ConsoleMessage } from "../../../types/workflow-common.interface";
import { ExecutionState } from "src/app/workspace/types/execute-workflow.interface";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CdkVirtualScrollViewport } from "@angular/cdk/scrolling";
import { presetPalettes } from "@ant-design/colors";
import { isDefined } from "../../../../common/util/predicate";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";

@UntilDestroy()
@Component({
  selector: "texera-console-frame",
  templateUrl: "./console-frame.component.html",
  styleUrls: ["./console-frame.component.scss"],
})
export class ConsoleFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;
  @ViewChild(CdkVirtualScrollViewport) viewPort?: CdkVirtualScrollViewport;
  @ViewChild("consoleList", { read: ElementRef }) listElement?: ElementRef;

  // display error message:
  errorMessages?: Readonly<Record<string, string>>;

  // display print
  consoleMessages: ReadonlyArray<ConsoleMessage> = [];

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
  ]);

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowConsoleService: WorkflowConsoleService,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
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
        if (
          event.previous.state === ExecutionState.BreakpointTriggered &&
          event.current.state === ExecutionState.Completed
        ) {
          // intentionally do nothing to leave the information displayed as it is
          // when kill a workflow after hitting breakpoint
        } else if (
          event.previous.state === ExecutionState.Initializing &&
          event.current.state === ExecutionState.Running
        ) {
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
  }

  clearConsole(): void {
    this.consoleMessages = [];
    this.errorMessages = undefined;
  }

  renderConsole(): void {
    // try to fetch if we have breakpoint info
    const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();

    if (this.operatorId) {
      this.workerIds = this.executeWorkflowService.getWorkerIds(this.operatorId);

      // first display error messages if applicable
      if (this.operatorId === breakpointTriggerInfo?.operatorID) {
        // if we hit a breakpoint
        this.displayBreakpoint(breakpointTriggerInfo);
      } else {
        // otherwise we assume it's a fault
        this.displayFault();
      }

      // always display console messages
      this.displayConsoleMessages(this.operatorId);
    }
  }

  displayBreakpoint(breakpointTriggerInfo: BreakpointTriggerInfo): void {
    const errorsMessages: Record<string, string> = {};
    breakpointTriggerInfo.report.forEach(r => {
      const splitPath = r.actorPath.split("/");
      const workerName = splitPath[splitPath.length - 1];
      const workerText = "Worker " + workerName + ":                ";
      if (r.messages.toString().toLowerCase().includes("exception")) {
        errorsMessages[workerText] = r.messages.toString();
      }
    });
    this.errorMessages = errorsMessages;
  }

  displayFault(): void {
    this.errorMessages = this.executeWorkflowService.getErrorMessages();
  }

  displayConsoleMessages(operatorId: string): void {
    this.consoleMessages = operatorId ? this.workflowConsoleService.getConsoleMessages(operatorId) || [] : [];

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
}

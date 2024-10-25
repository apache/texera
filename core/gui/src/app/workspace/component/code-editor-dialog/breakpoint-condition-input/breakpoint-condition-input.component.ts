import {
  AfterViewChecked,
  Component,
  ElementRef,
  EventEmitter,
  HostListener,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
import { UdfDebugService } from "../../../service/operator-debug/udf-debug.service";
import { isDefined } from "../../../../common/util/predicate";
import { MonacoEditor } from "monaco-breakpoints/dist/types";

/**
 * This component is a dialog that allows users to input a condition for a breakpoint.
 */
@Component({
  selector: "texera-breakpoint-condition-input",
  templateUrl: "./breakpoint-condition-input.component.html",
  styleUrls: ["./breakpoint-condition-input.component.scss"],
})
export class BreakpointConditionInputComponent implements OnChanges {
  constructor(private udfDebugService: UdfDebugService) {}

  @Input() operatorId = "";
  @Input() lineNum?: number;
  @Input() monacoEditor!: MonacoEditor;
  @Output() closeEmitter = new EventEmitter<void>();
  public condition = "";
  public topPosition: string = "0px";
  public leftPosition: string = "0px";
  ngOnChanges(changes: SimpleChanges): void {
    if (!isDefined(changes["lineNum"]?.currentValue)) {
      return;
    }
    // when the line number changes, update the condition
    this.condition = this.udfDebugService.getCondition(this.operatorId, this.lineNum!) ?? "";

    // update position
    const layoutInfo = this.monacoEditor.getLayoutInfo();
    const editorRect = this.monacoEditor.getDomNode()?.getBoundingClientRect();
    const topValue =
      (editorRect?.top || 0) +
      this.monacoEditor.getBottomForLineNumber(this.lineNum!) -
      this.monacoEditor.getScrollTop();
    const leftValue = (editorRect?.left || 0) + (layoutInfo?.glyphMarginLeft || 0) - 160;
    this.topPosition = `${topValue}px`;
    this.leftPosition = `${leftValue}px`;
  }

  public left(): number {
    if (!isDefined(this.monacoEditor)) {
      return 0;
    }

    // Calculate the left position of the input popup based on the editor layout
    const { glyphMarginLeft } = this.monacoEditor.getLayoutInfo()!;
    const { left } = this.monacoEditor.getDomNode()!.getBoundingClientRect();
    return left + glyphMarginLeft - this.monacoEditor.getScrollLeft() - 160;
  }

  public top(): number {
    if (!(isDefined(this.monacoEditor) && isDefined(this.lineNum))) {
      return 0;
    }

    // Calculate the top position of the input popup based on the editor layout
    const topPixel = this.monacoEditor.getBottomForLineNumber(this.lineNum);
    const editorRect = this.monacoEditor.getDomNode()?.getBoundingClientRect();
    return (editorRect?.top || 0) + topPixel - this.monacoEditor.getScrollTop();
  }

  get isVisible(): boolean {
    return isDefined(this.lineNum);
  }

  /**
   * Update the condition and close the dialog when the user presses Enter or focus out.
   * @param event the keyboard event, or undefined if the event is focus out.
   */
  @HostListener("window:keydown", ["$event"])
  @HostListener("focusout")
  handleEvent(event?: KeyboardEvent): void {
    if (!this.lineNum || (event && !(event.key === "Enter" && !event.shiftKey))) {
      // perform no changes if no line number or the key is not Enter
      return;
    }

    // prevent the default behavior of the Enter key
    event?.preventDefault();

    // save the updated condition
    this.udfDebugService.doUpdateBreakpointCondition(this.operatorId, this.lineNum, this.condition.trim());

    // close the dialog
    this.closeEmitter.emit();
  }
}

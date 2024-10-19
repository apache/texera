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
import * as monaco from "monaco-editor";
import { MonacoEditor } from "monaco-breakpoints/dist/types";

@Component({
  selector: "texera-breakpoint-condition-input",
  templateUrl: "./breakpoint-condition-input.component.html",
  styleUrls: ["./breakpoint-condition-input.component.scss"],
})
export class BreakpointConditionInputComponent implements AfterViewChecked, OnChanges {
  @Input() operatorId = "";
  @Input() lineNum?: number;
  @Input() monacoEditor!: MonacoEditor;
  @Output() closeEmitter = new EventEmitter<void>();

  @ViewChild("conditionTextarea") textarea!: ElementRef<HTMLTextAreaElement>;


  left(): number {
    if (isDefined(this.monacoEditor)) {
      const { glyphMarginLeft } = this.monacoEditor.getLayoutInfo()!;
      const { left: editorLeft, top: editorTop } = this.monacoEditor.getDomNode()!.getBoundingClientRect();
      return editorLeft + glyphMarginLeft - this.monacoEditor.getScrollLeft();
    }
    return 0;
  };
  top(): number {
    if (isDefined(this.monacoEditor) && isDefined(this.lineNum)){
      const topPixel = this.monacoEditor.getTopForLineNumber(this.lineNum);
      const lineHeight = this.monacoEditor.getOption(monaco.editor.EditorOption.lineHeight);
      const { left: editorLeft, top: editorTop } = this.monacoEditor.getDomNode()!.getBoundingClientRect();
      return editorTop + topPixel + lineHeight / 2 - this.monacoEditor.getScrollTop();
    }
    return 0;
  }
  condition = "";

  constructor(private udfDebugService: UdfDebugService) {

  }


  isVisible(): boolean {
    return isDefined(this.lineNum);
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["lineNum"]?.currentValue) {
      const manager = this.udfDebugService.getOrCreateManager(this.operatorId);
      this.condition = manager.getCondition(this.lineNum!) ?? "";
    }
  }

  ngAfterViewChecked(): void {
    if (this.isVisible()) {
      this.textarea?.nativeElement.focus();
    }
  }

  @HostListener("window:keydown", ["$event"])
  @HostListener("focusout")
  handleEvent(event?: KeyboardEvent): void {
    if (!event || (event.key === "Enter" && !event.shiftKey)) {
      event?.preventDefault(); // Prevent default only for Enter key event
      this.updateConditionAndClose();
    }
  }

  private updateConditionAndClose(): void {
    if (!this.lineNum) {
      return;
    }

    this.udfDebugService.doUpdateBreakpointCondition(this.operatorId, this.lineNum, this.condition.trim());
    this.closeEmitter.emit();
  }
}

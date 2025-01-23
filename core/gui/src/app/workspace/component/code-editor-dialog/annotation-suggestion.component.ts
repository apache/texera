import { Component, Input, Output, EventEmitter } from "@angular/core";

@Component({
  selector: "texera-annotation-suggestion",
  templateUrl: "./annotation-suggestion.component.html",
  styleUrls: ["./annotation-suggestion.component.scss"],
})
export class AnnotationSuggestionComponent {
  @Input() code: string = "";
  @Input() suggestion: string = "";
  @Input() top: number = 0;
  @Input() left: number = 0;
  @Output() accept = new EventEmitter<void>();
  @Output() decline = new EventEmitter<void>();

  onAccept() {
    this.accept.emit();
  }

  onDecline() {
    this.decline.emit();
  }
}

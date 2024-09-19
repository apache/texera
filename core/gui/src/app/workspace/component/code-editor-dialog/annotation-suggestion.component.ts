import { Component, Input, Output, EventEmitter } from "@angular/core";

@Component({
  selector: "texera-annotation-suggestion",
  template: `
    <div
      class="annotation-suggestion"
      [style.top.px]="top"
      [style.left.px]="left">
      <p>Do you agree with the type annotation suggestion?</p>
      <pre>Adding annotation for code: {{ code }}</pre>
      <p>
        Given suggestion: <strong>{{ suggestion }}</strong>
      </p>
      <button
        class="accept-button"
        (click)="onAccept()">
        Accept
      </button>
      <button
        class="decline-button"
        (click)="onDecline()">
        Decline
      </button>
    </div>
  `,
  styles: [
    `
      .annotation-suggestion {
        position: absolute;
        background: #222;
        color: #fff;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.5);
        z-index: 1000;
      }

      .annotation-suggestion button {
        margin-right: 10px;
      }

      .annotation-suggestion button.accept-button {
        background-color: #28a745;
        color: #000;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        cursor: pointer;
      }

      .annotation-suggestion button.accept-button:hover {
        background-color: #218838;
      }

      .annotation-suggestion button.decline-button {
        background-color: #dc3545;
        color: #000;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        cursor: pointer;
      }

      .annotation-suggestion button.decline-button:hover {
        background-color: #c82333;
      }
    `,
  ],
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

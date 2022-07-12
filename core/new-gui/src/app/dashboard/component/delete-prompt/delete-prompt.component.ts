import { Component, Input } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";

@Component({
  selector: "texera-delete-prompt",
  templateUrl: "./delete-prompt.component.html",
  styleUrls: ["./delete-prompt.component.css"],
})
export class DeletePromptComponent {
  @Input() deletionType!: string;
  @Input() deletionName!: string;

  constructor(public activeModal: NgbActiveModal) {}

  /**
   * sends the user confirm to the main component. It does not call any method in service.
   */
  public delete(): void {
    this.activeModal.close(true);
  }
}

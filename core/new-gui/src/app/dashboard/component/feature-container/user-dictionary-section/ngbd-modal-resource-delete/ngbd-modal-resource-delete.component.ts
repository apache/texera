import { Component, Input, Output, EventEmitter } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";

import { UserDictionary } from "../../../../../common/type/user-dictionary";

/**
 * NgbdModalResourceDeleteComponent is the pop-up
 * component for undoing the delete. User may cancel
 * a dictionary deletion.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: "texera-resource-section-delete-dict-modal",
  templateUrl: "./ngbd-modal-resource-delete.component.html",
  styleUrls: ["./ngbd-modal-resource-delete.component.scss", "../../../dashboard.component.scss"],
})
export class NgbdModalResourceDeleteComponent {
  public dictionary: UserDictionary = {
    name: "",
    id: -1,
    items: [],
    description: "",
  };

  constructor(public activeModal: NgbActiveModal) {}

  /**
   * deleteDictionary sends the user confirm to the frontend to delete
   * a certain dictionary in user storage.
   *
   * @param
   */
  public deleteDictionary(): void {
    this.activeModal.close(true);
  }
}

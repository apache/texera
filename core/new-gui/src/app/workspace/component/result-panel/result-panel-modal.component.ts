import { Component, Input } from "@angular/core";
import { NzModalRef } from "ng-zorro-antd/modal";

/**
 *
 * NgbModalComponent is the pop-up window that will be
 *  displayed when the user clicks on a specific row
 *  to show the displays of that row.
 *
 * User can exit the pop-up window by
 *  1. Clicking the dismiss button on the top-right hand corner
 *      of the Modal
 *  2. Clicking the `Close` button at the bottom-right
 *  3. Clicking any shaded area that is not the pop-up window
 *  4. Pressing `Esc` button on the keyboard
 */
@Component({
  selector: "texera-row-modal-content",
  templateUrl: "./result-panel-modal.component.html",
  styleUrls: ["./result-panel.component.scss"],
})
export class RowModalComponent {
  // when modal is opened, currentDisplayRow will be passed as
  //  componentInstance to this NgbModalComponent to display
  //  as data table.
  @Input() currentDisplayRowData: Record<string, unknown> = {};

  // Index of currentDisplayRowData in currentResult
  @Input() currentDisplayRowIndex: number = 0;

  constructor(public modal: NzModalRef<any, number>) {}
}

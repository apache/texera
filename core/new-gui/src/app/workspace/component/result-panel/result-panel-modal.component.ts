import { Component, Input, OnChanges } from "@angular/core";
import { NzModalRef } from "ng-zorro-antd/modal";
import { trimDisplayJsonData } from "src/app/common/util/json";
import { DEFAULT_PAGE_SIZE, WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { PRETTY_JSON_TEXT_LIMIT } from "./result-table-frame/result-table-frame.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

/**
 *
 * The pop-up window that will be
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
@UntilDestroy()
@Component({
  selector: "texera-row-modal-content",
  templateUrl: "./result-panel-modal.component.html",
  styleUrls: ["./result-panel.component.scss"],
})
export class RowModalComponent implements OnChanges {
  // Index of current displayed row in currentResult
  @Input() operatorId?: string;
  @Input() rowIndex: number = 0;

  // when modal is opened, currentDisplayRow will be passed as
  //  componentInstance to display as data table.
  currentDisplayRowData: Record<string, unknown> = {};

  constructor(
    public modal: NzModalRef<any, number>,
    private workflowResultService: WorkflowResultService
  ) {}

  ngOnChanges(): void {
    if (this.operatorId !== undefined) {
      const resultService = this.workflowResultService.getPaginatedResultService(this.operatorId);
      resultService
        ?.selectTuple(this.rowIndex, DEFAULT_PAGE_SIZE)
        .pipe(untilDestroyed(this))
        .subscribe(res => {
          this.currentDisplayRowData = trimDisplayJsonData(res, PRETTY_JSON_TEXT_LIMIT);
        });
    }
  }
}

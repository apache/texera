import { Component } from "@angular/core";
import Fuse from "fuse.js";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { GroupInfo, OperatorSchema } from "../../../types/operator-schema.interface";
import { DragDropService } from "../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzAutocompleteOptionComponent } from "ng-zorro-antd/auto-complete";

@UntilDestroy()
@Component({
  selector: "texera-operator-menu",
  templateUrl: "operator-menu.component.html",
  styleUrls: ["operator-menu.component.scss"],
})
export class OperatorMenuComponent {
  public opList = new Map<string, Array<OperatorSchema>>();
  public groupNames: ReadonlyArray<GroupInfo> = [];

  // input value of the search input box
  public searchInputValue: string = "";
  // search autocomplete suggestion list
  public autocompleteOptions: OperatorSchema[] = [];

  public canModify = true;

  // fuzzy search using fuse.js. See parameters in options at https://fusejs.io/
  public fuse = new Fuse([] as ReadonlyArray<OperatorSchema>, {
    shouldSort: true,
    threshold: 0.3,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["additionalMetadata.userFriendlyName"],
  });

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private dragDropService: DragDropService
  ) {
    // clear the search box if an operator is dropped from operator search box
    this.dragDropService.operatorDropStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.searchInputValue = "";
      this.autocompleteOptions = [];
    });
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => (this.canModify = canModify));
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(operatorMetadata => {
        const ops = operatorMetadata.operators.filter(
          operatorSchema => operatorSchema.operatorType !== "PythonUDF" && operatorSchema.operatorType !== "Dummy"
        );
        this.groupNames = operatorMetadata.groups;
        ops.forEach(x => {
          const group = x.additionalMetadata.operatorGroupName;
          const list = this.opList.get(group) || [];
          list.push(x);
          this.opList.set(group, list);
        });
        this.opList.forEach(value => {
          value.sort((a, b) => a.operatorType.localeCompare(b.operatorType));
        });
        this.fuse.setCollection(ops);
      });
  }

  /**
   * create the search results observable
   * whenever the search box text is changed, perform the search using fuse.js
   */
  onInput(e: Event): void {
    const v = (e.target as HTMLInputElement).value;
    if (v === null || v.trim().length === 0) {
      this.autocompleteOptions = [];
    }
    this.autocompleteOptions = this.fuse.search(v).map(item => {
      return item.item;
    });
  }

  /**
   * handles the event when an operator search option is selected.
   * adds the operator to the canvas and clears the text in the search box
   */
  onSelectionChange(e: NzAutocompleteOptionComponent): void {
    const selectSchema = e.nzValue as OperatorSchema;
    // add the operator to the graph on select (position relative to the current viewpoint)
    const origin = this.workflowActionService.getJointGraphWrapper().getMainJointPaper()?.translate();
    const point = { x: 400 - (origin?.tx ?? 0), y: 200 - (origin?.ty ?? 0) };
    this.workflowActionService.addOperator(
      this.workflowUtilService.getNewOperatorPredicate(selectSchema.operatorType),
      point
    );

    // asynchronously immediately clear the search input and suggestions
    // because ng-zorro shows the selected value if it's synchronously
    setTimeout(() => {
      this.searchInputValue = "";
      this.autocompleteOptions = [];
    }, 0);
  }
}

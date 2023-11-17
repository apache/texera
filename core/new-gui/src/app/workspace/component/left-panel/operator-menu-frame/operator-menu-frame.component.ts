import { Component, OnInit } from "@angular/core";
import Fuse from "fuse.js";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { GroupInfo, OperatorMetadata, OperatorSchema } from "../../../types/operator-schema.interface";
import { DragDropService } from "../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzAutocompleteOptionComponent } from "ng-zorro-antd/auto-complete";

/**
 * OperatorMenuFrameComponent is a panel that shows the operators.
 *
 * This component gets all the operator metadata from OperatorMetaDataService,
 *  and then displays the operators, which are grouped using their group name from the metadata.
 *
 * Clicking a group name reveals the operators in the group, each operator is a sub-component: OperatorLabelComponent,
 *  this is implemented using Angular Material's expansion panel component: https://material.angular.io/components/expansion/overview
 *
 * OperatorMenuFrameComponent also includes a search box, which uses fuse.js to support fuzzy search on operator names.
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-operator-panel",
  templateUrl: "./operator-menu-frame.component.html",
  styleUrls: ["./operator-menu-frame.component.scss"],
  providers: [
    // uncomment this line for manual testing without opening backend server
    // { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
  ],
})
export class OperatorMenuFrameComponent implements OnInit {
  // a list of all operator's schema
  public operatorSchemaList: ReadonlyArray<OperatorSchema> = [];
  // a list of group names, sorted based on the groupOrder from OperatorMetadata
  public groupNamesOrdered: ReadonlyArray<string> = [];
  // a map of group name to a list of operator schema of this group
  public operatorGroupMap = new Map<string, ReadonlyArray<OperatorSchema>>();

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
    this.dragDropService
      .getOperatorDropStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (OperatorLabelComponent.isOperatorLabelElementFromSearchBox(event.dragElementID)) {
          this.searchInputValue = "";
          this.autocompleteOptions = [];
        }
      });
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => {
        this.canModify = canModify;
      });
  }

  ngOnInit() {
    // subscribe to the operator metadata changed observable and process it
    // the operator metadata will be fetched asynchronously on application init
    //   after the data is fetched, it will be passed through this observable
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(value => this.processOperatorMetadata(value));
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

  /**
   * populate the class variables based on the operator metadata fetched from the backend:
   *  - sort the group names based on the group order
   *  - put the operators into the hashmap of group names
   *
   * @param operatorMetadata metadata of all operators
   */
  private processOperatorMetadata(operatorMetadata: OperatorMetadata): void {
    operatorMetadata = {
      ...operatorMetadata,
      operators: operatorMetadata.operators.filter(operatorSchema => operatorSchema.operatorType != "PythonUDF"),
    };
    this.operatorSchemaList = operatorMetadata.operators;
    this.groupNamesOrdered = getGroupNamesSorted(operatorMetadata.groups);
    this.operatorGroupMap = getOperatorGroupMap(operatorMetadata);
    this.fuse.setCollection(this.operatorSchemaList);
  }
}

/**
 * generates a list of group names sorted by the order
 * slice() will make a copy of the list, because we don't want to sort the original list
 */
export function getGroupNamesSorted(groupInfoList: ReadonlyArray<GroupInfo>): string[] {
  return groupInfoList
    .slice()
    .sort((a, b) => a.groupOrder - b.groupOrder)
    .map(groupInfo => groupInfo.groupName);
}

/**
 * returns a new empty map from the group name to a list of OperatorSchema
 */
export function getOperatorGroupMap(operatorMetadata: OperatorMetadata): Map<string, OperatorSchema[]> {
  const groups = operatorMetadata.groups.map(groupInfo => groupInfo.groupName);
  const operatorGroupMap = new Map<string, OperatorSchema[]>();
  groups.forEach(groupName => {
    const operators = operatorMetadata.operators.filter(x => x.additionalMetadata.operatorGroupName === groupName);
    operatorGroupMap.set(groupName, operators);
  });
  return operatorGroupMap;
}

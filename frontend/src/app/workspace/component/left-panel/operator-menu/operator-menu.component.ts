/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component } from "@angular/core";
import { combineLatest } from "rxjs";
import { MacroService } from "../../../service/macro/macro.service";
import { macroType } from "../../../../common/util/workflow-macro";
import Fuse from "fuse.js";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { GroupInfo, OperatorSchema } from "../../../types/operator-schema.interface";
import { DragDropService } from "../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  NzAutocompleteOptionComponent,
  NzAutocompleteTriggerDirective,
  NzAutocompleteComponent,
} from "ng-zorro-antd/auto-complete";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputDirective } from "ng-zorro-antd/input";
import { FormsModule } from "@angular/forms";
import { NgFor, NgTemplateOutlet } from "@angular/common";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { MacroPanelComponent } from "./macro-panel.component";

@UntilDestroy()
@Component({
  selector: "texera-operator-menu",
  templateUrl: "operator-menu.component.html",
  styleUrls: ["operator-menu.component.scss"],
  imports: [
    MacroPanelComponent,
    NzSpaceCompactItemDirective,
    NzInputDirective,
    FormsModule,
    NzAutocompleteTriggerDirective,
    NzAutocompleteComponent,
    NgFor,
    NzAutocompleteOptionComponent,
    OperatorLabelComponent,
    NgTemplateOutlet,
    NzCollapseComponent,
    NzCollapsePanelComponent,
  ],
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
    private dragDropService: DragDropService,
    private macros: MacroService
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
    combineLatest([this.operatorMetadataService.getOperatorMetadata(), this.macros.library$])
      .pipe(untilDestroyed(this))
      .subscribe(([operatorMetadata, definitions]) => {
        const ops = operatorMetadata.operators.filter(
          operatorSchema => operatorSchema.operatorType !== "PythonUDF" && operatorSchema.operatorType !== "Dummy"
        );
        this.groupNames = operatorMetadata.groups;
        this.opList.clear();
        ops.forEach(x => {
          if (x.operatorType !== "Sleep") {
            const group = x.additionalMetadata.operatorGroupName;
            const list = this.opList.get(group) || [];
            list.push(x);
            this.opList.set(group, list);
          }
        });
        this.opList.forEach(value => {
          value.sort((a, b) => a.operatorType.localeCompare(b.operatorType));
        });
        this.fuse.setCollection([
          ...ops,
          ...definitions.map(d =>
            this.operatorMetadataService.getOperatorSchema(macroType(d), [d, ...(d.content.macroDependencies ?? [])])
          ),
        ]);
        this.search(this.searchInputValue);
      });
  }

  /**
   * create the search results observable
   * whenever the search box text is changed, perform the search using fuse.js
   */
  onInput(e: Event): void {
    this.search((e.target as HTMLInputElement).value);
  }

  private search(value: string): void {
    this.autocompleteOptions = this.fuse.search(value).map(item => item.item);
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

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

import { UntilDestroy } from "@ngneat/until-destroy";
import { AfterViewInit, Component, EventEmitter, Input, Output, ViewChild } from "@angular/core";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../../../../common/type/datasetVersionFileTree";
import { ITreeOptions, TREE_ACTIONS } from "@ali-hm/angular-tree-component";

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-version-filetree",
  templateUrl: "./user-dataset-version-filetree.component.html",
  styleUrls: ["./user-dataset-version-filetree.component.scss"],
})
export class UserDatasetVersionFiletreeComponent implements AfterViewInit {
  @Input()
  public isTreeNodeDeletable: boolean = false;

  @Input()
  public fileTreeNodes: DatasetFileNode[] = [];

  @Input()
  public isExpandAllAfterViewInit = false;

  @ViewChild("tree") tree: any;

  public fileTreeDisplayOptions: ITreeOptions = {
    displayField: "name",
    hasChildrenField: "children",
    actionMapping: {
      mouse: {
        click: (tree: any, node: any, $event: any) => {
          if (node.hasChildren) {
            TREE_ACTIONS.TOGGLE_EXPANDED(tree, node, $event);
          } else {
            this.selectedTreeNode.emit(node.data);
          }
        },
      },
    },
  };

  @Output()
  public selectedTreeNode = new EventEmitter<DatasetFileNode>();

  @Output()
  public deletedTreeNode = new EventEmitter<DatasetFileNode>();

  constructor() {}

  onNodeDeleted(node: DatasetFileNode): void {
    // look up for the DatasetVersionFileTreeNode
    this.deletedTreeNode.emit(node);
  }

  ngAfterViewInit(): void {
    if (this.isExpandAllAfterViewInit) {
      this.tree.treeModel.expandAll();
    }
  }
}

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
import {
  DatasetFileNode,
  getRelativePathFromDatasetFileNode,
} from "../../../../../../common/type/datasetVersionFileTree";
import { ITreeOptions, TREE_ACTIONS, TreeModule } from "@ali-hm/angular-tree-component";
import { NgIf } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";

const IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".gif", ".webp"] as const;

// The tree library lays rows out on a fixed pitch: nodeHeight plus a 2px drop
// slot after every row (its default dropSlotHeight), with one extra leading
// drop slot before the first row.
const TREE_DROP_SLOT_HEIGHT_PX = 2;

// Cap on the container height; matches the max-height the container had
// before virtualization.
const MAX_FILE_TREE_CONTAINER_HEIGHT_PX = 200;

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-version-filetree",
  templateUrl: "./user-dataset-version-filetree.component.html",
  styleUrls: ["./user-dataset-version-filetree.component.scss"],
  imports: [
    TreeModule,
    NgIf,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzTooltipDirective,
  ],
})
export class UserDatasetVersionFiletreeComponent implements AfterViewInit {
  @Input()
  public isTreeNodeDeletable: boolean = false;

  @Input()
  public set fileTreeNodes(nodes: DatasetFileNode[]) {
    this._fileTreeNodes = nodes ?? [];
    const newHeight = this.computeContainerHeightPx();
    if (newHeight !== this.fileTreeContainerHeightPx) {
      this.fileTreeContainerHeightPx = newHeight;
      // The tree measures its viewport height once after init and again only
      // on scroll. Both hosts create this component with an empty tree and
      // fill it when data arrives, so ask the tree to re-measure once change
      // detection has applied the new container height — otherwise the 0px
      // initial measurement sticks and the tree renders blank. The same
      // one-shot measurement means a host must never instantiate this
      // component inside a hidden (display: none) container without calling
      // tree.sizeChanged() on reveal.
      setTimeout(() => this.tree?.sizeChanged());
    }
  }
  public get fileTreeNodes(): DatasetFileNode[] {
    return this._fileTreeNodes;
  }
  private _fileTreeNodes: DatasetFileNode[] = [];

  @Input()
  public isExpandAllAfterViewInit = false;

  @ViewChild("tree") tree: any;

  @Output()
  setCoverImage = new EventEmitter<string>();

  // Row height (px) used by the tree's virtual scroll to position rows. The
  // template binds it onto the container as --tree-node-height, which the
  // SCSS row rules consume, so rows can never drift from the nodeHeight the
  // tree positions them with.
  public readonly TREE_NODE_HEIGHT_PX = 24;

  // Container height bound in the template: hugs the content for small trees
  // (the pre-virtualization behavior) and caps at 200px for large ones so the
  // virtual scroll has a definite, bounded viewport.
  public fileTreeContainerHeightPx = 0;

  // useVirtualScroll keeps only the visible rows in the DOM; without it a
  // version with hundreds of files renders one component per file and every
  // change-detection pass freezes the page for seconds to minutes.
  public fileTreeDisplayOptions: ITreeOptions = {
    displayField: "name",
    hasChildrenField: "children",
    useVirtualScroll: true,
    nodeHeight: this.TREE_NODE_HEIGHT_PX,
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

  isImageFile(fileName: string): boolean {
    return IMAGE_EXTENSIONS.some(ext => fileName.toLowerCase().endsWith(ext));
  }

  // Content height for the whole tree at the library's row pitch. The count
  // includes collapsed descendants, so a partially collapsed folder tree may
  // get a container slightly taller than its visible rows — still bounded by
  // the cap, and exact for the flat file lists both hosts show.
  private computeContainerHeightPx(): number {
    const nodeCount = UserDatasetVersionFiletreeComponent.countNodes(this._fileTreeNodes);
    if (nodeCount === 0) {
      return 0;
    }
    const contentHeightPx =
      nodeCount * (this.TREE_NODE_HEIGHT_PX + TREE_DROP_SLOT_HEIGHT_PX) + TREE_DROP_SLOT_HEIGHT_PX;
    return Math.min(contentHeightPx, MAX_FILE_TREE_CONTAINER_HEIGHT_PX);
  }

  private static countNodes(nodes: DatasetFileNode[]): number {
    let count = 0;
    for (const node of nodes) {
      count += 1 + (node.children ? UserDatasetVersionFiletreeComponent.countNodes(node.children) : 0);
    }
    return count;
  }

  onSetCover(nodeData: DatasetFileNode): void {
    this.setCoverImage.emit(getRelativePathFromDatasetFileNode(nodeData));
  }
}

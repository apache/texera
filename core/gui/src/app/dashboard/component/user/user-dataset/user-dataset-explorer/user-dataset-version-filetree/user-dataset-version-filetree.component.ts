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

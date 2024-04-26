import { UntilDestroy } from "@ngneat/until-destroy";
import {
  AfterViewInit,
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
import {
  DatasetVersionFileTreeNode,
  getFullPathFromFileTreeNode,
} from "../../../../../../common/type/datasetVersionFileTree";
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
  public fileTreeNodes: DatasetVersionFileTreeNode[] = [];

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
  public selectedTreeNode = new EventEmitter<DatasetVersionFileTreeNode>();

  @Output()
  public deletedTreeNode = new EventEmitter<DatasetVersionFileTreeNode>();

  constructor() {}

  onNodeDeleted(node: DatasetVersionFileTreeNode): void {
    // look up for the DatasetVersionFileTreeNode
    this.deletedTreeNode.emit(node);
  }

  ngAfterViewInit(): void {
    if (this.isExpandAllAfterViewInit) {
      this.tree.treeModel.expandAll();
    }
  }
}

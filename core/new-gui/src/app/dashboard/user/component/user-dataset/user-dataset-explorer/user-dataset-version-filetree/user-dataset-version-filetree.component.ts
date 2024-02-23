import { UntilDestroy } from "@ngneat/until-destroy";
import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChanges} from "@angular/core";
// import { ITreeOptions, TREE_ACTIONS } from "@circlon/angular-tree-component";
import {DatasetVersionFileTreeNode, getFullPathFromFileTreeNode} from "../../../../../../common/type/datasetVersionFileTree";
import {NzFormatEmitEvent, NzTreeNode} from "ng-zorro-antd/tree";
import {NzContextMenuService, NzDropdownMenuComponent} from "ng-zorro-antd/dropdown";
import {SelectionModel} from "@angular/cdk/collections";
import {FlatTreeControl} from "@angular/cdk/tree";
import {NzTreeFlatDataSource, NzTreeFlattener} from "ng-zorro-antd/tree-view";

interface TreeFlatNode {
  expandable: boolean;
  name: string;
  level: number;
  disabled: boolean;
  key: string;
}

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-version-filetree",
  templateUrl: "./user-dataset-version-filetree.component.html",
  styleUrls: ["./user-dataset-version-filetree.component.scss"],
})
export class UserDatasetVersionFiletreeComponent implements OnInit, OnChanges{

  @Input()
  isTreeNodeDeletable: boolean = true;

  @Input()
  fileTreeNodes: DatasetVersionFileTreeNode[] = [];

  @Output()
  public selectedTreeNode = new EventEmitter<DatasetVersionFileTreeNode>();

  @Output()
  public deletedTreeNode = new EventEmitter<DatasetVersionFileTreeNode>();


  nodeLookup: { [key: string]: DatasetVersionFileTreeNode } = {};

  treeNodeTransformer = (node: DatasetVersionFileTreeNode, level: number): TreeFlatNode => {
    const uniqueKey = getFullPathFromFileTreeNode(node); // Or any other unique identifier logic
    this.nodeLookup[uniqueKey] = node; // Store the node in the lookup table
    return {
      expandable: !!node.children && node.children.length > 0 && node.type == "directory",
      name: node.name,
      level,
      disabled: false,
      key: uniqueKey
    }
  }

  selectListSelection = new SelectionModel<TreeFlatNode>();
  treeControl = new FlatTreeControl<TreeFlatNode>(
    node => node.level,
    node => node.expandable
  )
  treeFlattener = new NzTreeFlattener(
    this.treeNodeTransformer,
    node => node.level,
    node => node.expandable,
    node => node.children
  );

  dataSource = new NzTreeFlatDataSource(this.treeControl, this.treeFlattener);
  constructor(private nzContextMenuService: NzContextMenuService) {}

  hasChild = (_: number, node: TreeFlatNode): boolean => node.expandable;
  ngOnInit(): void {
    this.nodeLookup = {};
    this.dataSource.setData(this.fileTreeNodes);
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.nodeLookup = {};
    this.dataSource.setData(this.fileTreeNodes);
  }

  onNodeSelected(node: TreeFlatNode): void {
    // look up for the DatasetVersionFileTreeNode
    this.selectedTreeNode.emit(this.nodeLookup[node.key])
  }
  onNodeDeleted(node: TreeFlatNode): void {
    // look up for the DatasetVersionFileTreeNode
    this.deletedTreeNode.emit(this.nodeLookup[node.key])
  }

}

import { UntilDestroy } from "@ngneat/until-destroy";
import { Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChanges } from "@angular/core";
import {
  DatasetVersionFileTreeNode,
  getFullPathFromFileTreeNode,
} from "../../../../../../common/type/datasetVersionFileTree";
import { NzContextMenuService } from "ng-zorro-antd/dropdown";
import { SelectionModel } from "@angular/cdk/collections";
import { NzTreeFlatDataSource, NzTreeFlattener } from "ng-zorro-antd/tree-view";
import { FlatTreeControl, TreeControl } from "@angular/cdk/tree";

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
export class UserDatasetVersionFiletreeComponent implements OnInit, OnChanges {
  @Input()
  public isTreeNodeDeletable: boolean = false;

  @Input()
  public fileTreeNodes: DatasetVersionFileTreeNode[] = [];

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
      key: uniqueKey,
    };
  };

  selectListSelection = new SelectionModel<TreeFlatNode>();
  treeControl = new FlatTreeControl<TreeFlatNode>(
    node => node.level,
    node => node.expandable
  );
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
    // this delay is used to make user not be able to see the shuffling tree
  }

  ngOnChanges(changes: SimpleChanges): void {
    // Track expanded nodes
    const expandedKeys = this.treeControl.dataNodes
      .filter(node => this.treeControl.isExpanded(node))
      .map(node => node.key);

    // Update nodeLookup and data source
    this.nodeLookup = {};

    this.dataSource.setData(this.fileTreeNodes);

    // Re-expand previously expanded nodes
    this.treeControl.dataNodes.forEach(node => {
      if (expandedKeys.includes(node.key)) {
        this.treeControl.expand(node);
      }
    });
  }

  onNodeSelected(node: TreeFlatNode): void {
    // look up for the DatasetVersionFileTreeNode
    this.selectedTreeNode.emit(this.nodeLookup[node.key]);
  }
  onNodeDeleted(node: TreeFlatNode): void {
    // look up for the DatasetVersionFileTreeNode
    this.deletedTreeNode.emit(this.nodeLookup[node.key]);
  }
}

import { UntilDestroy } from "@ngneat/until-destroy";
import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChanges} from "@angular/core";
// import { ITreeOptions, TREE_ACTIONS } from "@circlon/angular-tree-component";
import {DatasetVersionFileTreeNode, getFullPathFromFileTreeNode} from "../../../../../../common/type/datasetVersionFileTree";
import {NzFormatEmitEvent, NzTreeNode} from "ng-zorro-antd/tree";
import {NzContextMenuService, NzDropdownMenuComponent} from "ng-zorro-antd/dropdown";

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-version-filetree",
  templateUrl: "./user-dataset-version-filetree.component.html",
  styleUrls: ["./user-dataset-version-filetree.component.scss"],
})
export class UserDatasetVersionFiletreeComponent implements OnInit, OnChanges{

  @Input()
  fileTreeNodes: DatasetVersionFileTreeNode[] = [];

  @Output()
  public selectedTreeNode = new EventEmitter<DatasetVersionFileTreeNode>();

  @Output()
  public deletedTreeNode = new EventEmitter<DatasetVersionFileTreeNode>();

  nodes: NzTreeNode[] = [];

  nodeLookup: { [key: string]: DatasetVersionFileTreeNode } = {};


  constructor(private nzContextMenuService: NzContextMenuService) {}

  onNodeSelected(data: NzFormatEmitEvent): void {
    const treeNode = data.node!;
    // look up for the DatasetVersionFileTreeNode
    this.selectedTreeNode.emit(this.nodeLookup[treeNode.key])
  }

  contextMenu($event: MouseEvent, menu: NzDropdownMenuComponent): void {
    this.nzContextMenuService.create($event, menu);
  }

  openFolder(data: NzTreeNode | NzFormatEmitEvent): void {
    // do something if u want
    if (data instanceof NzTreeNode) {
      data.isExpanded = !data.isExpanded;
    } else {
      const node = data.node;
      if (node) {
        node.isExpanded = !node.isExpanded;
      }
    }
  }

  selectDropdown(): void {
    // do something
  }

  ngOnInit(): void {
    this.nodeLookup = {};
    this.nodes = this.parseTreeNodesToNzTreeNodes(this.fileTreeNodes);
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.nodeLookup = {};
    this.nodes = this.parseTreeNodesToNzTreeNodes(this.fileTreeNodes);
  }

  parseTreeNodesToNzTreeNodes(nodes: DatasetVersionFileTreeNode[]): NzTreeNode[] {
    return nodes.map(node => {
      const uniqueKey = getFullPathFromFileTreeNode(node); // Or any other unique identifier logic
      this.nodeLookup[uniqueKey] = node; // Store the node in the lookup table

      return new NzTreeNode({
        title: node.name,
        key: uniqueKey, // key is the full path of the node
        isLeaf: node.type === 'file',
        children: node.children ? this.parseTreeNodesToNzTreeNodes(node.children) : [],
      });
    });
  }
}

import { Component, inject, Input } from "@angular/core";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { UntilDestroy } from "@ngneat/until-destroy";
import {
  DatasetVersionFileTreeManager,
  DatasetVersionFileTreeNode,
  EnvironmentDatasetFileNodes,
  getFullPathFromFileTreeNode,
} from "../../../common/type/datasetVersionFileTree";

@UntilDestroy()
@Component({
  selector: "texera-file-selection-model",
  templateUrl: "file-selection.component.html",
  styleUrls: ["file-selection.component.scss"],
})
export class FileSelectionComponent {
  readonly fileTreeNodes: ReadonlyArray<DatasetVersionFileTreeNode> = inject(NZ_MODAL_DATA).fileTreeNodes;
  suggestedFileTreeNodes: DatasetVersionFileTreeNode[] = [];
  filterText: string = "";

  constructor(private modalRef: NzModalRef) {}

  ngOnInit() {
    console.log(this.fileTreeNodes);
    this.suggestedFileTreeNodes = [...this.fileTreeNodes]; // Initially, suggested nodes are all nodes
  }

  filterFileTreeNodes() {
    const filterText = this.filterText.toLowerCase();

    if (!filterText) {
      this.suggestedFileTreeNodes = [...this.fileTreeNodes];
    } else {
      // Recursive function to filter nodes
      const filterNodes = (node: DatasetVersionFileTreeNode): DatasetVersionFileTreeNode | null => {
        // Check if the current node matches the filter text
        const fullPath = getFullPathFromFileTreeNode(node).toLowerCase();
        if (fullPath.includes(filterText)) {
          // If the node matches, return it as-is, including all its children
          return node;
        }

        // If the node is a directory, check its children
        if (node.type === "directory" && node.children) {
          const filteredChildren = node.children
            .map(child => filterNodes(child))
            .filter(child => child !== null) as DatasetVersionFileTreeNode[];

          if (filteredChildren.length > 0) {
            // If any children match, return the current node with filtered children
            return { ...node, children: filteredChildren };
          }
        }

        // If no match and no matching children, return null to indicate pruning
        return null;
      };

      // Apply filter to each root node and remove any that are null after filtering
      this.suggestedFileTreeNodes = this.fileTreeNodes
        .map(node => filterNodes(node))
        .filter(node => node !== null) as DatasetVersionFileTreeNode[];
    }
  }

  onFileTreeNodeSelected(node: DatasetVersionFileTreeNode) {
    const selectedNodePath = getFullPathFromFileTreeNode(node);
    console.log("before pruning: ", selectedNodePath)
    this.modalRef.close(selectedNodePath);
  }
}

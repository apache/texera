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
    this.suggestedFileTreeNodes = [...this.fileTreeNodes]; // Initially, suggested nodes are all nodes
  }

  filterFileTreeNodes() {
    const filterText = this.filterText.trim().toLowerCase();

    if (!filterText) {
      this.suggestedFileTreeNodes = [...this.fileTreeNodes];
    } else {
      const filterNodes = (node: DatasetVersionFileTreeNode): DatasetVersionFileTreeNode | null => {
        // For 'file' type nodes, check if the node's name matches the filter text.
        // Directories are not filtered out by name, but their children are filtered recursively.
        if (node.type === "file" && !node.name.toLowerCase().includes(filterText)) {
          return null; // Exclude files that don't match the filter.
        }

        // If the node is a directory, recurse into its children, if any.
        if (node.type === "directory" && node.children) {
          const filteredChildren = node.children
            .map(filterNodes)
            .filter(child => child !== null) as DatasetVersionFileTreeNode[];

          if (filteredChildren.length > 0) {
            // If any children match, return the current directory node with filtered children.
            return { ...node, children: filteredChildren };
          } else {
            // If no children match, exclude the directory node.
            return null;
          }
        }

        // Return the node if it's a file that matches or a directory with matching descendants.
        return node;
      };

      this.suggestedFileTreeNodes = this.fileTreeNodes
        .map(filterNodes)
        .filter(node => node !== null) as DatasetVersionFileTreeNode[];
    }
  }

  onFileTreeNodeSelected(node: DatasetVersionFileTreeNode) {
    const selectedNodePath = getFullPathFromFileTreeNode(node);
    this.modalRef.close(selectedNodePath);
  }
}

import {Component, inject, Input} from '@angular/core';
import {NZ_MODAL_DATA, NzModalRef} from 'ng-zorro-antd/modal';
import { UntilDestroy } from "@ngneat/until-destroy";
import {DatasetVersionFileTreeNode, EnvironmentDatasetFileNodes, getFullPathFromFileTreeNode} from "../../../common/type/datasetVersionFileTree";

@UntilDestroy()
@Component({
  selector: 'texera-file-selection-model',
  templateUrl: "file-selection.component.html",
  styleUrls: ["file-selection.component.scss"]
})
export class FileSelectionComponent {
  readonly fileTreeNodes: ReadonlyArray<DatasetVersionFileTreeNode> = inject(NZ_MODAL_DATA).fileTreeNodes;
  suggestedFileTreeNodes: DatasetVersionFileTreeNode[] = [];
  filterText: string = '';

  constructor(private modalRef: NzModalRef) {}

  ngOnInit() {
    this.suggestedFileTreeNodes = [...this.fileTreeNodes]; // Initially, suggested nodes are all nodes
  }

  filterFileTreeNodes() {
    if (!this.filterText) {
      this.suggestedFileTreeNodes = [...this.fileTreeNodes];
    } else {
      const lowerCaseFilterText = this.filterText.toLowerCase();
      this.suggestedFileTreeNodes = this.fileTreeNodes.filter(node =>
        getFullPathFromFileTreeNode(node).toLowerCase().includes(lowerCaseFilterText));
    }
  }

  onFileTreeNodeSelected(node: DatasetVersionFileTreeNode) {
    this.modalRef.close(getFullPathFromFileTreeNode(node));
  }
}

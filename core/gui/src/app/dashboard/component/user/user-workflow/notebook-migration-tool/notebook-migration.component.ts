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

import { Component, Input } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { NzModalService } from "ng-zorro-antd/modal";
import { JupyterNotebook, JupyterOutput } from "../../../../type/jupyter-notebook.interface";

@Component({
  selector: "texera-jupyter-upload-success",
  templateUrl: "./notebook-migration.component.html",
  styleUrls: ["./notebook-migration.component.scss"],
})
export class JupyterUploadSuccessComponent {
  @Input() notebookContent!: JupyterNotebook;
  datasets: File[] = [];
  popupStage: number = 0; // flag to manage content display
  workflowGeneratingInProgress: boolean = false;
  workflowJsonContent: any; // variable to store the JSON content

  constructor(
    private http: HttpClient,
    private modalService: NzModalService
  ) {}

  getOutputText(output: JupyterOutput): string {
    if (output.output_type === "stream") {
      return output.text ? output.text.join("") : "";
    } else if (output.output_type === "execute_result" || output.output_type === "display_data") {
      return output.data ? output.data["text/plain"] || "" : "";
    } else if (output.output_type === "error") {
      return output.traceback ? output.traceback.join("\n") : "";
    }
    return "";
  }

  onFileChange(event: any): void {
    const files: FileList = event.target.files;
    for (let i = 0; i < files.length; i++) {
      this.datasets.push(files[i]);
    }
  }

  convertToWorkflow(): void {
    this.workflowGeneratingInProgress = true;

    // Simulate workflow generation using mock data instead of an HTTP request
    setTimeout(() => {
      const mockData = {
        operators: [
          { id: "1", type: "OperatorA", properties: {} },
          { id: "2", type: "OperatorB", properties: {} },
        ],
        links: [{ source: "1", target: "2" }],
        commentBoxes: [],
        groups: [],
        operatorPositions: {
          "1": { x: 100, y: 200 },
          "2": { x: 300, y: 400 },
        },
      };

      // Process the mock data
      this.workflowJsonContent = mockData; // Assign mock data as workflow content
      this.workflowGeneratingInProgress = false;
    }, 2000); // Simulate a delay of 2 seconds for processing
  }

  /**
   * Process the selected datasets (CSV files)
   */
  private processDatasets(): void {
    if (this.datasets.length > 0) {
      this.datasets.forEach(file => {
        this.handleFileUploads(file);
      });
    }
  }

  /**
   * Function to handle file uploads and process them (CSV dataset)
   */
  private handleFileUploads(file: Blob): void {
    const reader = new FileReader();
    reader.readAsText(file);
    reader.onload = () => {
      try {
        const result = reader.result;
        if (typeof result !== "string") {
          throw new Error("Incorrect format: file is not a string");
        }
        const datasetContent = result; // Here we get the dataset content as CSV

        // Handle the dataset content as needed (e.g., send it to backend or use in workflow)
        console.log("Uploaded dataset content:", datasetContent);

        // Update the workflow with dataset processing if needed
        this.workflowJsonContent = { ...this.workflowJsonContent, datasetContent }; // Append dataset content

        // Optional: Proceed to the next step or close the modal, etc.
      } catch (error) {
        console.error("Error processing the file: ", error);
      }
    };
  }

  compareOutput(): void {
    console.log("Generating output to compare");
    this.popupStage = 2;
    // TODO: Implement the comparison logic here
  }
}

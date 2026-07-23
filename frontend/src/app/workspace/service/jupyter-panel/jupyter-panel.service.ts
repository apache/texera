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

import { Injectable } from "@angular/core";
import { catchError, map, of } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OperatorLink } from "../../types/workflow-common.interface";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { distinctUntilChanged, switchMap } from "rxjs/operators";
import { AppSettings } from "../../../common/app-setting";
import { NotebookMigrationService } from "../notebook-migration/notebook-migration.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";

@Injectable({
  providedIn: "root",
})
export class JupyterPanelService {
  private iframeRef: HTMLIFrameElement | null = null; // Store reference to iframe element

  // Precomputed dictionary for cell to highlight mapping
  private cellToHighlightMapping: Record<string, { components: string[]; edges: string[] }> = {};

  // Cached Jupyter server origin (see resolveJupyterOrigin)
  private jupyterOrigin: Promise<string | null> | null = null;

  constructor(
    private workflowActionService: WorkflowActionService,
    private http: HttpClient,
    private notebookMigrationService: NotebookMigrationService,
    private config: GuiConfigService
  ) {
    window.addEventListener("message", this.handleNotebookMessage);
  }

  private get enabled(): boolean {
    return this.config.env.pythonNotebookMigrationEnabled;
  }

  /**
   * Resolve and cache the Jupyter server origin, used both to validate incoming
   * iframe messages and as the postMessage target. The backend serves a
   * process-static base URL, so the origin is fixed for the app's lifetime.
   * A failed/unavailable lookup is not cached, so it can be retried once the
   * Jupyter pod becomes reachable.
   */
  private resolveJupyterOrigin(): Promise<string | null> {
    if (this.jupyterOrigin) {
      return this.jupyterOrigin;
    }
    const pending: Promise<string | null> = this.notebookMigrationService.getJupyterURL().then(url => {
      if (url) {
        try {
          return new URL(url).origin;
        } catch {
          /* malformed URL — fall through to retry */
        }
      }
      this.jupyterOrigin = null; // don't cache failures
      return null;
    });
    this.jupyterOrigin = pending;
    return pending;
  }

  public init(): void {
    if (!this.enabled) return;
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(
        map(meta => meta.wid),
        distinctUntilChanged()
      )
      .subscribe(wid => {
        // On every workflow change, drop the outgoing workflow's stale mapping
        // and clear the highlight index. Clearing here (not only inside
        // precomputeHighlightMapping, which runs only on a successful fetch)
        // ensures switching to a workflow without a stored notebook can't leave
        // the previous workflow's highlights active. This cleanup previously
        // happened inside closeJupyterNotebookPanel; the panel-visibility
        // surface lives with the iframe component in
        // `migration-tool-jupyter-panel` now, so it is inlined.
        const currentWid = this.workflowActionService.getWorkflow().wid;
        if (currentWid !== undefined) {
          this.notebookMigrationService.deleteMapping("mapping_wid_" + currentWid);
        }
        this.cellToHighlightMapping = {};
        // Skip unsaved workflows (wid undefined) and wid 0; both would POST
        // without a usable wid and 500 on the backend.
        if (wid) {
          this.fetchNotebookAndMapping(wid).subscribe(result => {
            if (result == 1) {
              this.precomputeHighlightMapping();
              // Panel auto-open on workflow restore is wired in
              // `migration-tool-jupyter-panel` once the visibility API exists.
            }
          });
        }
      });
  }

  private fetchNotebookAndMapping(
    workflowID: number | undefined = this.workflowActionService.getWorkflow().wid,
    vId: number = 1
  ) {
    // Fetch mapping and notebook from migration database if exists for wid
    const dbAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/fetch-notebook-and-mapping`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });
    const payload = {
      wid: workflowID,
      vid: vId, // Future work: add dynamic fetching of current workflow vId
    };

    return this.http.post(dbAPIUrl, payload, { headers }).pipe(
      switchMap(async (response: any) => {
        // Only load mapping and workflow if they exist
        if (response.exists) {
          this.notebookMigrationService.setMapping("mapping_wid_" + workflowID, response.mapping);

          if ((await this.notebookMigrationService.sendNotebookToJupyter(response.notebook)) == 1) {
            return 1;
          } else {
            return 0;
          }
        } else {
          return 0;
        }
      }),
      catchError((error: unknown) => {
        console.error("Network response was not ok when fetching notebook and mapping:", error);
        return of(0);
      })
    );
  }

  // Precompute the dictionary for O(1) highlighting
  private precomputeHighlightMapping(): void {
    // Rebuild from scratch so entries from a previously opened workflow don't linger.
    this.cellToHighlightMapping = {};

    const wid = this.workflowActionService.getWorkflow().wid;

    if (wid === undefined) {
      console.warn("Workflow ID is undefined. Cannot compute highlight mapping.");
      return;
    }
    const mappingKey = "mapping_wid_" + wid;
    const mapping = this.notebookMigrationService.getMapping(mappingKey);

    if (mapping == undefined) {
      console.warn(`Mapping key '${mappingKey}' not found. Cannot compute highlight mapping.`);
      return;
    }
    const cellToOperator = mapping.cell_to_operator;

    const allLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();

    for (const cellUUID in cellToOperator) {
      const components = cellToOperator[cellUUID] || [];
      const componentSet = new Set(components);
      const edges: string[] = [];

      allLinks.forEach(link => {
        const sourceOperatorID = link.source.operatorID;
        const targetOperatorID = link.target.operatorID;

        if (
          componentSet.has(sourceOperatorID) &&
          componentSet.has(targetOperatorID) &&
          sourceOperatorID !== targetOperatorID
        ) {
          edges.push(link.linkID);
        }
      });

      this.cellToHighlightMapping[cellUUID] = { components, edges };
    }
  }

  // Set the iframe reference (from the component's ViewChild). The panel
  // component that calls this lives in `migration-tool-jupyter-panel`.
  setIframeRef(iframe: HTMLIFrameElement) {
    this.iframeRef = iframe;
  }

  // Handle messages from the Jupyter notebook iframe
  private handleNotebookMessage = async (event: MessageEvent) => {
    if (!this.enabled) return;

    // Only accept messages posted by our own notebook iframe. This is the
    // strong check: it rejects any other same-origin frame or script trying to
    // drive highlighting with a synthetic cellClicked message.
    if (!this.iframeRef || event.source !== this.iframeRef.contentWindow) {
      return;
    }

    // Defense in depth: also require the message origin to match the resolved
    // Jupyter origin.
    const jupyterOrigin = await this.resolveJupyterOrigin();
    if (!jupyterOrigin || event.origin !== jupyterOrigin) {
      return;
    }

    const { action, cellUUID } = event.data ?? {};
    if (action === "cellClicked") {
      this.highlightFromCell(cellUUID);
    }
  };

  // Highlight operators and edges based on the clicked cell
  private highlightFromCell(cellUUID: string): void {
    const highlightData = this.cellToHighlightMapping[cellUUID] || { components: [], edges: [] };

    // Unhighlight all operators and links
    this.workflowActionService.unhighlightOperators(
      ...this.workflowActionService
        .getTexeraGraph()
        .getAllOperators()
        .map(op => op.operatorID)
    );
    this.workflowActionService.unhighlightLinks(
      ...this.workflowActionService
        .getTexeraGraph()
        .getAllLinks()
        .map(link => link.linkID)
    );

    // Highlight components and edges
    if (highlightData.components.length > 0) {
      this.workflowActionService.highlightOperators(true, ...highlightData.components);
    }
    if (highlightData.edges.length > 0) {
      this.workflowActionService.highlightLinks(true, ...highlightData.edges);
    }
  }

  // Handle when a Texera operator is clicked to trigger the corresponding notebook cell(s)
  async onWorkflowComponentClick(operatorId: string): Promise<void> {
    if (!this.enabled) return;
    const jupyterOrigin = await this.resolveJupyterOrigin();
    if (jupyterOrigin && this.iframeRef && this.iframeRef.contentWindow) {
      const wid = this.workflowActionService.getWorkflow().wid;

      if (wid == undefined) {
        console.error("Error fetching wid of current workflow");
        return;
      }

      const mappingKey = "mapping_wid_" + wid;
      const mappingEntry = this.notebookMigrationService.getMapping(mappingKey);

      if (!mappingEntry) {
        console.error("Missing mapping for workflow:", mappingKey);
        return;
      }

      const cellIds = mappingEntry["operator_to_cell"][operatorId];
      if (cellIds && cellIds.length > 0) {
        // "operators" is the payload key custom.js expects; the values are the
        // mapped cell UUIDs for the clicked operator.
        this.iframeRef.contentWindow.postMessage({ action: "triggerCellClick", operators: cellIds }, jupyterOrigin);
      } else {
        console.error(`No cells mapped to operator: ${operatorId}`);
      }
    }
  }
}

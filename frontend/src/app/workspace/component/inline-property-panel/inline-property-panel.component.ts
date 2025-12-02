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

import { Component, EventEmitter, Input, OnChanges, Output, SimpleChanges } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPredicate } from "../../types/workflow-common.interface";

/**
 * Represents a key-value property to display in the panel.
 */
export interface PropertyItem {
  label: string;
  value: string;
  icon?: string;
}

/**
 * InlinePropertyPanelComponent displays key properties for non-code operators.
 * It shows a compact list of important properties like file paths, attributes, limits, etc.
 */
@UntilDestroy()
@Component({
  selector: "texera-inline-property-panel",
  templateUrl: "./inline-property-panel.component.html",
  styleUrls: ["./inline-property-panel.component.scss"],
})
export class InlinePropertyPanelComponent implements OnChanges {
  @Input() operatorId!: string;
  @Input() displayName: string = "Properties";

  @Output() closePanel = new EventEmitter<string>();

  properties: PropertyItem[] = [];
  operatorIcon: string = "setting";

  constructor(private workflowActionService: WorkflowActionService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["operatorId"]) {
      this.updateProperties();
    }
  }

  onClose(): void {
    this.closePanel.emit(this.operatorId);
  }

  private updateProperties(): void {
    if (!this.operatorId) {
      this.properties = [];
      return;
    }

    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.operatorId);
    if (!operator) {
      this.properties = [];
      return;
    }

    this.properties = this.extractProperties(operator);
    this.operatorIcon = this.getOperatorIcon(operator.operatorType);
  }

  private extractProperties(operator: OperatorPredicate): PropertyItem[] {
    const props = operator.operatorProperties as Record<string, any>;
    const type = operator.operatorType;

    switch (type) {
      case "Projection":
        return this.extractProjectionProperties(props);
      case "Sort":
        return this.extractSortProperties(props);
      case "Limit":
        return this.extractLimitProperties(props);
      case "CSVScanSource":
      case "CSVFileScan":
        return this.extractCSVScanProperties(props);
      case "HashJoin":
        return this.extractHashJoinProperties(props);
      default:
        return this.extractGenericProperties(props);
    }
  }

  private extractProjectionProperties(props: Record<string, any>): PropertyItem[] {
    const items: PropertyItem[] = [];
    const isDrop = props["isDrop"] ?? false;

    items.push({
      label: "Mode",
      value: isDrop ? "Drop" : "Keep",
      icon: isDrop ? "minus-circle" : "check-circle",
    });

    // Note: Projection operator uses "originalAttribute" property, not "attribute"
    const attributes = props["attributes"] as Array<{ originalAttribute?: string; alias?: string }> | undefined;
    if (attributes && attributes.length > 0) {
      const attrNames = attributes
        .map(a => {
          const original = a.originalAttribute || "";
          const alias = a.alias;
          return alias && alias !== original ? `${original} → ${alias}` : original;
        })
        .filter(Boolean);
      items.push({
        label: "Attributes",
        value: attrNames.join(", ") || "(none)",
        icon: "unordered-list",
      });
    }

    return items;
  }

  private extractSortProperties(props: Record<string, any>): PropertyItem[] {
    const items: PropertyItem[] = [];
    // Note: Sort operator uses "attribute" property, not "attributeName"
    const attributes = props["attributes"] as Array<{ attribute?: string; sortPreference?: string }> | undefined;

    if (attributes && attributes.length > 0) {
      const sortSpec = attributes
        .map(a => {
          const name = a.attribute || "";
          const order = a.sortPreference === "DESC" ? "↓" : "↑";
          return `${name} ${order}`;
        })
        .filter(Boolean);
      items.push({
        label: "Sort By",
        value: sortSpec.join(", ") || "(none)",
        icon: "sort-ascending",
      });
    }

    return items;
  }

  private extractLimitProperties(props: Record<string, any>): PropertyItem[] {
    const items: PropertyItem[] = [];
    const limit = props["limit"];

    if (limit !== undefined && limit !== null) {
      items.push({
        label: "Limit",
        value: String(limit),
        icon: "number",
      });
    }

    return items;
  }

  private extractCSVScanProperties(props: Record<string, any>): PropertyItem[] {
    const items: PropertyItem[] = [];

    const fileName = props["fileName"];
    if (fileName) {
      // Extract just the filename from the path
      const shortName = this.getShortFileName(fileName);
      items.push({
        label: "File",
        value: shortName,
        icon: "file-text",
      });
    }

    const delimiter = props["customDelimiter"];
    if (delimiter) {
      items.push({
        label: "Delimiter",
        value: delimiter === "," ? "comma" : delimiter === "\t" ? "tab" : `"${delimiter}"`,
        icon: "column-width",
      });
    }

    const hasHeader = props["hasHeader"];
    if (hasHeader !== undefined) {
      items.push({
        label: "Header",
        value: hasHeader ? "Yes" : "No",
        icon: "table",
      });
    }

    return items;
  }

  private extractHashJoinProperties(props: Record<string, any>): PropertyItem[] {
    const items: PropertyItem[] = [];

    const leftAttr = props["buildAttributeName"];
    const rightAttr = props["probeAttributeName"];
    const joinType = props["joinType"];

    if (leftAttr && rightAttr) {
      items.push({
        label: "Join",
        value: `${leftAttr} = ${rightAttr}`,
        icon: "link",
      });
    }

    if (joinType) {
      const typeDisplay = String(joinType).toLowerCase().replace(/_/g, " ");
      items.push({
        label: "Type",
        value: typeDisplay,
        icon: "merge-cells",
      });
    }

    return items;
  }

  private extractGenericProperties(props: Record<string, any>): PropertyItem[] {
    const items: PropertyItem[] = [];

    // Show first few simple properties
    const keys = Object.keys(props).slice(0, 3);
    for (const key of keys) {
      const value = props[key];
      if (value !== undefined && value !== null && typeof value !== "object") {
        items.push({
          label: this.formatLabel(key),
          value: String(value),
        });
      }
    }

    return items;
  }

  private getShortFileName(path: string): string {
    // Handle URI format
    if (path.includes("/")) {
      const parts = path.split("/");
      return parts[parts.length - 1] || path;
    }
    return path;
  }

  private formatLabel(key: string): string {
    // Convert camelCase to Title Case
    return key
      .replace(/([A-Z])/g, " $1")
      .replace(/^./, str => str.toUpperCase())
      .trim();
  }

  private getOperatorIcon(operatorType: string): string {
    switch (operatorType) {
      case "Projection":
        return "select";
      case "Sort":
        return "sort-ascending";
      case "Limit":
        return "number";
      case "CSVScanSource":
      case "CSVFileScan":
        return "file-text";
      case "HashJoin":
        return "link";
      default:
        return "setting";
    }
  }
}

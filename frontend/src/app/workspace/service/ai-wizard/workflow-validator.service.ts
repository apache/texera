/**
 * Workflow validator. Ported from TexeraHackathon but uses Texera's live
 * OperatorMetadata for the operator catalog (design-doc §4.2: "operator
 * catalog 不允许手工维护 JSON").
 */

import { Injectable } from "@angular/core";
import { WorkflowContent } from "../../../common/type/workflow";
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { OperatorMetadata } from "../../types/operator-schema.interface";
import { ValidationError, ValidationResult } from "./types";

@Injectable({ providedIn: "root" })
export class WorkflowValidatorService {
  public validate(workflow: WorkflowContent, catalog: OperatorMetadata | null): ValidationResult {
    const errors: ValidationError[] = [];
    const warnings: string[] = [];

    if (!workflow.operators || !Array.isArray(workflow.operators)) {
      errors.push({ field: "operators", message: "Workflow must contain an operators array" });
      return { isValid: false, errors, warnings };
    }
    if (!workflow.links || !Array.isArray(workflow.links)) {
      errors.push({ field: "links", message: "Workflow must contain a links array" });
    }
    if (!workflow.operatorPositions || typeof workflow.operatorPositions !== "object") {
      errors.push({ field: "operatorPositions", message: "Workflow must contain operatorPositions object" });
    }
    if (!workflow.settings) {
      warnings.push("Missing workflow settings - using defaults");
    }

    const validTypes = new Set<string>(catalog?.operators.map(op => op.operatorType) ?? []);
    const operatorIds = new Set<string>();

    workflow.operators.forEach((op, i) => {
      errors.push(...this.validateOperator(op, i, validTypes, catalog));
      if (op.operatorID) {
        if (operatorIds.has(op.operatorID)) {
          errors.push({
            field: `operators[${i}].operatorID`,
            message: `Duplicate operator ID: ${op.operatorID}`,
          });
        }
        operatorIds.add(op.operatorID);
      }
    });

    if (workflow.operatorPositions) {
      for (const opId of operatorIds) {
        const pos = workflow.operatorPositions[opId];
        if (!pos) {
          errors.push({ field: "operatorPositions", message: `Missing position for operator: ${opId}` });
        } else if (typeof pos.x !== "number" || typeof pos.y !== "number") {
          errors.push({
            field: `operatorPositions[${opId}]`,
            message: "Position must have numeric x and y coordinates",
          });
        }
      }
    }

    const linkIds = new Set<string>();
    (workflow.links ?? []).forEach((link, i) => {
      errors.push(...this.validateLink(link, i, operatorIds, workflow.operators));
      if (link.linkID) {
        if (linkIds.has(link.linkID)) {
          errors.push({ field: `links[${i}].linkID`, message: `Duplicate link ID: ${link.linkID}` });
        }
        linkIds.add(link.linkID);
      }
    });

    warnings.push(...this.validateConnectivity(workflow.operators, workflow.links ?? [], catalog));

    return {
      isValid: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  private validateOperator(
    op: OperatorPredicate,
    i: number,
    validTypes: Set<string>,
    catalog: OperatorMetadata | null
  ): ValidationError[] {
    const errors: ValidationError[] = [];
    if (!op.operatorID) {
      errors.push({ field: `operators[${i}].operatorID`, message: "Operator must have an operatorID" });
    }
    if (!op.operatorType) {
      errors.push({ field: `operators[${i}].operatorType`, message: "Operator must have an operatorType" });
    } else if (catalog && !validTypes.has(op.operatorType)) {
      errors.push({
        field: `operators[${i}].operatorType`,
        message: `Unknown operator type: ${op.operatorType}. Must be one of the registered operators.`,
      });
    }
    if (!op.operatorProperties || typeof op.operatorProperties !== "object") {
      errors.push({
        field: `operators[${i}].operatorProperties`,
        message: "Operator must have operatorProperties object",
      });
    }
    if (!Array.isArray(op.inputPorts)) {
      errors.push({ field: `operators[${i}].inputPorts`, message: "Operator must have inputPorts array" });
    }
    if (!Array.isArray(op.outputPorts)) {
      errors.push({ field: `operators[${i}].outputPorts`, message: "Operator must have outputPorts array" });
    }

    // Required-property check using the live operator schema.
    if (op.operatorType && catalog) {
      const schema = catalog.operators.find(s => s.operatorType === op.operatorType);
      const required: string[] = Array.isArray((schema?.jsonSchema as any)?.required)
        ? ((schema?.jsonSchema as any).required as string[])
        : [];
      for (const prop of required) {
        if (op.operatorProperties && !(prop in op.operatorProperties)) {
          errors.push({
            field: `operators[${i}].operatorProperties.${prop}`,
            message: `Missing required property: ${prop}`,
          });
        }
      }
    }
    return errors;
  }

  private validateLink(
    link: OperatorLink,
    i: number,
    operatorIds: Set<string>,
    operators: readonly OperatorPredicate[]
  ): ValidationError[] {
    const errors: ValidationError[] = [];
    if (!link.linkID) errors.push({ field: `links[${i}].linkID`, message: "Link must have a linkID" });

    if (!link.source?.operatorID || !link.source?.portID) {
      errors.push({
        field: `links[${i}].source`,
        message: "Link must have valid source with operatorID and portID",
      });
    } else {
      if (!operatorIds.has(link.source.operatorID)) {
        errors.push({
          field: `links[${i}].source.operatorID`,
          message: `Source operator not found: ${link.source.operatorID}`,
        });
      } else {
        const sourceOp = operators.find(o => o.operatorID === link.source.operatorID);
        if (sourceOp && !sourceOp.outputPorts.some(p => p.portID === link.source.portID)) {
          errors.push({
            field: `links[${i}].source.portID`,
            message: `Source port ${link.source.portID} not found in operator ${link.source.operatorID}`,
          });
        }
      }
    }

    if (!link.target?.operatorID || !link.target?.portID) {
      errors.push({
        field: `links[${i}].target`,
        message: "Link must have valid target with operatorID and portID",
      });
    } else {
      if (!operatorIds.has(link.target.operatorID)) {
        errors.push({
          field: `links[${i}].target.operatorID`,
          message: `Target operator not found: ${link.target.operatorID}`,
        });
      } else {
        const targetOp = operators.find(o => o.operatorID === link.target.operatorID);
        if (targetOp && !targetOp.inputPorts.some(p => p.portID === link.target.portID)) {
          errors.push({
            field: `links[${i}].target.portID`,
            message: `Target port ${link.target.portID} not found in operator ${link.target.operatorID}`,
          });
        }
      }
    }
    return errors;
  }

  private validateConnectivity(
    operators: readonly OperatorPredicate[],
    links: readonly OperatorLink[],
    catalog: OperatorMetadata | null
  ): string[] {
    const warnings: string[] = [];
    const incoming = new Map<string, number>();
    const outgoing = new Map<string, number>();
    for (const link of links) {
      outgoing.set(link.source.operatorID, (outgoing.get(link.source.operatorID) ?? 0) + 1);
      incoming.set(link.target.operatorID, (incoming.get(link.target.operatorID) ?? 0) + 1);
    }
    for (const op of operators) {
      const schema = catalog?.operators.find(s => s.operatorType === op.operatorType);
      const inputCount = schema?.additionalMetadata.inputPorts.length ?? op.inputPorts.length;
      const outputCount = schema?.additionalMetadata.outputPorts.length ?? op.outputPorts.length;
      const category = schema?.additionalMetadata.operatorGroupName ?? "";
      const hasIncoming = incoming.has(op.operatorID);
      const hasOutgoing = outgoing.has(op.operatorID);

      if (inputCount === 0 && hasIncoming) {
        warnings.push(`Source operator ${op.operatorID} has incoming links`);
      }
      if (inputCount > 0 && !hasIncoming) {
        warnings.push(`Operator ${op.operatorID} has no incoming links`);
      }
      if (outputCount > 0 && !hasOutgoing && !category.toLowerCase().includes("visualization")) {
        warnings.push(`Operator ${op.operatorID} has no outgoing links`);
      }
    }
    return warnings;
  }
}

import { Component, Input, OnChanges, OnInit } from "@angular/core";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import {
  AttributeType,
  SchemaAttribute,
  SchemaPropagationService,
} from "../../../service/dynamic-schema/schema-propagation/schema-propagation.service";
import { filter, map } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

// correspond to operator type specified in backend OperatorDescriptor
export const TYPE_CASTING_OPERATOR_TYPE = "TypeCasting";

@UntilDestroy()
@Component({
  selector: "texera-type-casting-display",
  templateUrl: "./type-casting-display.component.html",
})
export class TypeCastingDisplayComponent implements OnInit, OnChanges {
  @Input() currentOperatorId: string | undefined;

  schemaToDisplay: Partial<SchemaAttribute>[] = [];
  displayTypeCastingSchemaInformation: boolean = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    private schemaPropagationService: SchemaPropagationService
  ) {}

  ngOnInit(): void {
    this.registerTypeCastingPropertyChangeHandler();
    this.registerInputSchemaChangeHandler();
  }

  // invoke on first init and every time the input binding is changed
  ngOnChanges(): void {
    if (!this.currentOperatorId) {
      this.displayTypeCastingSchemaInformation = false;
      return;
    }
    const op = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
    if (op.operatorType !== TYPE_CASTING_OPERATOR_TYPE) {
      this.displayTypeCastingSchemaInformation = false;
      return;
    }
    this.displayTypeCastingSchemaInformation = true;
    this.rerender();
  }

  registerTypeCastingPropertyChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPropertyChangeStream()
      .pipe(
        filter(op => op.operator.operatorID === this.currentOperatorId),
        filter(op => op.operator.operatorType === TYPE_CASTING_OPERATOR_TYPE),
        map(event => event.operator)
      )
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.rerender();
      });
  }

  private registerInputSchemaChangeHandler() {
    this.schemaPropagationService
      .getOperatorInputSchemaChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.rerender();
      });
  }

  rerender(): void {
    if (!this.currentOperatorId) {
      return;
    }
    this.schemaToDisplay = [];
    const inputSchema = this.schemaPropagationService.getOperatorInputSchema(this.currentOperatorId);

    const operatorPredicate = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);

    const castUnits: ReadonlyArray<{ attribute: string; resultType: AttributeType }> =
      operatorPredicate.operatorProperties["typeCastingUnits"] ?? [];

    const castTypeMap: Map<string, AttributeType> = new Map(castUnits.map(unit => [unit.attribute, unit.resultType]));
    inputSchema?.forEach(schema =>
      schema?.forEach(attr => {
        if (castTypeMap.has(attr.attributeName)) {
          const castedAttr: Partial<SchemaAttribute> = {
            attributeName: attr.attributeName,
            attributeType: castTypeMap.get(attr.attributeName),
          };
          this.schemaToDisplay.push(castedAttr);
        } else {
          this.schemaToDisplay.push(attr);
        }
      })
    );
  }
}

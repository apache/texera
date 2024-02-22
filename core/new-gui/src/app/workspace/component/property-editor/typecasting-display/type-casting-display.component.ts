import { Component, Input, OnChanges, OnInit } from "@angular/core";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import {
  SchemaAttribute,
  SchemaPropagationService,
} from "src/app/workspace/service/dynamic-schema/schema-propagation/schema-propagation.service";
import { OperatorPredicate } from "src/app/workspace/types/workflow-common.interface";
import { filter, map } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

// correspond to operator type specified in backend OperatorDescriptor
export const TYPE_CASTING_OPERATOR_TYPE = "TypeCasting";

@UntilDestroy()
@Component({
  selector: "texera-type-casting-display",
  templateUrl: "./type-casting-display.component.html",
  styleUrls: ["./type-casting-display.component.scss"],
})
export class TypeCastingDisplayComponent implements OnInit, OnChanges {
  @Input() currentOperatorId: string | undefined;

  schemaToDisplay: Partial<SchemaAttribute>[] = [];
  columnNamesToDisplay: string[] = ["attributeName", "attributeType"];
  displayTypeCastingSchemaInformation: boolean = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    private schemaPropagationService: SchemaPropagationService
  ) {}

  ngOnInit(): void {
    this.registerTypeCastingPropertyChangeHandler();
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
    this.updateComponent(op);
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
      .subscribe(op => {
        this.updateComponent(op);
      });
  }

  updateComponent(op: OperatorPredicate): void {
    if (!this.currentOperatorId) {
      return;
    }
    this.schemaToDisplay = [];
    const inputSchema = this.schemaPropagationService.getOperatorInputSchema(this.currentOperatorId);

    const castTypeMap =
      op.operatorProperties["typeCastingUnits"] ??
      [].reduce(
        (map_: { [x: string]: any }, castTo: { attribute: string; resultType: string }) => (
          (map_[castTo.attribute] = castTo.resultType), map_
        ),
        {}
      );

    inputSchema?.forEach(schema =>
      schema?.forEach(attr => {
        if (attr.attributeName in castTypeMap) {
          const castedAttr: Partial<SchemaAttribute> = {
            attributeName: attr.attributeName,
            attributeType: castTypeMap[attr.attributeName],
          };
          this.schemaToDisplay.push(castedAttr);
        } else {
          this.schemaToDisplay.push(attr);
        }
      })
    );
  }
}

import { Component, Input, OnChanges } from '@angular/core';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';
import {
  SchemaAttribute,
  SchemaPropagationService
} from 'src/app/workspace/service/dynamic-schema/schema-propagation/schema-propagation.service';
import { OperatorPredicate } from 'src/app/workspace/types/workflow-common.interface';

// correspond to operator type specified in backend OperatorDescriptor
export const TYPE_CASTING_OPERATOR_TYPE = 'TypeCasting';

@Component({
  selector: 'texera-type-casting-display',
  templateUrl: './type-casting-display.component.html',
  styleUrls: ['./type-casting-display.component.scss']
})


export class TypeCastingDisplayComponent implements OnChanges {

  public schemaToDisplay: Partial<SchemaAttribute>[] = [];
  public columnNamesToDisplay: string[] = ['attributeName', 'attributeType'];
  public displayTypeCastingSchemaInformation: boolean = false;

  @Input() operatorID: string | undefined;

  constructor(
    private workflowActionService: WorkflowActionService,
    private schemaPropagationService: SchemaPropagationService,
  ) {
    this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
      .filter(op => op.operator.operatorID === this.operatorID)
      .filter(op => op.operator.operatorType === TYPE_CASTING_OPERATOR_TYPE)
      .map(event => event.operator)
      .subscribe(op => {
        this.updateComponent(op);
      });
  }

  // invoke on first init and every time the input binding is changed
  ngOnChanges(): void {
    if (!this.operatorID) {
      this.displayTypeCastingSchemaInformation = false;
      return;
    }
    const op = this.workflowActionService.getTexeraGraph().getOperator(this.operatorID);
    if (op.operatorType !== TYPE_CASTING_OPERATOR_TYPE) {
      this.displayTypeCastingSchemaInformation = false;
      return;
    }
    this.displayTypeCastingSchemaInformation = true;
    this.updateComponent(op);
  }

  private updateComponent(op: OperatorPredicate): void {

    if (!this.operatorID) {
      return;
    }
    this.schemaToDisplay = [];
    const inputSchema = this.schemaPropagationService.getOperatorInputSchema(this.operatorID);

    const castTypeMap = op.operatorProperties['typeCastingUnits']
      .reduce((map: { [x: string]: any; }, castTo: { attribute: string; resultType: string; }) =>
        (map[castTo.attribute] = castTo.resultType, map), {});

    inputSchema?.forEach(schema => schema?.forEach(attr => {
      if (attr.attributeName in castTypeMap) {
        const castedAttr: Partial<SchemaAttribute> = {
          attributeName: attr.attributeName,
          attributeType: castTypeMap[attr.attributeName]
        };
        this.schemaToDisplay.push(castedAttr);
      } else {
        this.schemaToDisplay.push(attr);
      }
    }));

  }
}




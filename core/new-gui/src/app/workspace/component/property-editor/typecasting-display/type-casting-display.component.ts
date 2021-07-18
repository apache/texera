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

  public schema: SchemaAttribute[] = [];
  public displayedColumns: string[] = ['attributeName', 'attributeType'];
  public showTypeCastingTypeInformation: boolean = false;

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
      this.showTypeCastingTypeInformation = false;
      return;
    }
    const op = this.workflowActionService.getTexeraGraph().getOperator(this.operatorID);
    if (op.operatorType !== TYPE_CASTING_OPERATOR_TYPE) {
      this.showTypeCastingTypeInformation = false;
      return;
    }
    this.showTypeCastingTypeInformation = true;
    this.updateComponent(op);
  }

  private updateComponent(op: OperatorPredicate): void {

    if (!this.operatorID) {
      return;
    }
    this.schema = [];
    const inputSchema = this.schemaPropagationService.getOperatorInputSchema(this.operatorID);
    inputSchema?.forEach(schema => schema?.forEach(attr => {
      const castedAttr = {...attr};
      for (const castTo of op.operatorProperties['TypeCasting Units']) {
        if (castTo.attribute === attr.attributeName) {
          castedAttr.attributeType = castTo.resultType;
        }
      }
      this.schema.push(castedAttr);
    }));

  }
}




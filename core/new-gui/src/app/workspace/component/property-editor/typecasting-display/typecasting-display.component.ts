import { Component, Input, OnInit, OnChanges } from '@angular/core';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';
import { SchemaPropagationService } from 'src/app/workspace/service/dynamic-schema/schema-propagation/schema-propagation.service';
import { OperatorPredicate } from 'src/app/workspace/types/workflow-common.interface';

// correspond to operator type specified in backend OperatorDescriptor
export const TYPE_CASTING_OPERATOR_TYPE = 'TypeCasting';

@Component({
  selector: 'texera-typecasting-display',
  templateUrl: './typecasting-display.component.html',
  styleUrls: ['./typecasting-display.component.scss']
})
export class TypecastingDisplayComponent implements OnInit, OnChanges {

  public attribute: string | undefined;
  public inputType: string | undefined;
  public resultType: string | undefined;

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

  ngOnInit(): void {
  }

  // invoke on first init and every time the input binding is changed
  ngOnChanges(): void {
    if (! this.operatorID) {
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
    this.attribute = op.operatorProperties['attribute'];
    this.resultType = op.operatorProperties['resultType'];
    if (! this.operatorID) {
      return;
    }
    const inputSchema = this.schemaPropagationService.getOperatorInputSchema(this.operatorID);
    if (! inputSchema || inputSchema.length === 0) {
      return;
    }
    const inputSchemaPort0 = inputSchema[0];
    if (! inputSchemaPort0) {
      return;
    }
    this.inputType = inputSchemaPort0.find(e => e.attributeName === this.attribute)?.attributeType;
  }


}

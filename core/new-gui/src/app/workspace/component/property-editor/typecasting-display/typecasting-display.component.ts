import { Component, Input, OnInit } from '@angular/core';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';
import { SchemaPropagationService } from 'src/app/workspace/service/dynamic-schema/schema-propagation/schema-propagation.service';

@Component({
  selector: 'texera-typecasting-display',
  templateUrl: './typecasting-display.component.html',
  styleUrls: ['./typecasting-display.component.scss']
})
export class TypecastingDisplayComponent implements OnInit {

  public inputType: string | undefined;
  public showTypeCastingTypeInformation: boolean = false;

  @Input() data: any = {};
  @Input() operatorID: string = '';

  constructor(
    private workflowActionService: WorkflowActionService,
    private schemaPropagationService: SchemaPropagationService,
  ) {
  }

  ngOnInit(): void {
  }


  public TypeCastingTypeInformation(operatorID: string) {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (operator.operatorType === 'TypeCasting') {
      this.showTypeCastingTypeInformation = true;
    } else {
      this.showTypeCastingTypeInformation = false;
    }
    return this.showTypeCastingTypeInformation;
  }

  public getInputType(operatorID: string): string | undefined {
    const inputType: string | undefined = this.schemaPropagationService.getOperatorInputSchema(operatorID) ?
      .filter(e => e.attributeName === this.data.attribute).map(e => e.attributeType)[0] ;
    return inputType;
  }

}

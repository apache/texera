import { Component, Input, OnInit } from '@angular/core';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';

@Component({
  selector: 'texera-typecasting-display',
  templateUrl: './typecasting-display.component.html',
  styleUrls: ['./typecasting-display.component.scss']
})
export class TypecastingDisplayComponent implements OnInit {
  public inputType: string|undefined;
  @Input() data: any = {};
  @Input() operatorID: string = '';
  constructor(
    private workflowActionService: WorkflowActionService
  ) {

  }

  ngOnInit(): void {
    this.inputType=this.getInputType(this.operatorID)
  }

  public getInputType(operatorID: string):string|undefined {

    var operatorAttributeAndTypeArray = this.workflowActionService.getOperatorIdToSchemaAttributeMap(operatorID)
    var inputType:string|undefined = operatorAttributeAndTypeArray?.filter(e=>
      e.attributeName==this.data.attribute
    ).map(e=> e.attributeType)[0]
    return inputType
  }

}

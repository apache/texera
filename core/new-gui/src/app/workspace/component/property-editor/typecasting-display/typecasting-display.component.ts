import { Component, Input, OnInit, OnChanges } from '@angular/core';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';
import { SchemaPropagationService } from 'src/app/workspace/service/dynamic-schema/schema-propagation/schema-propagation.service';

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
      .filter(op => op.operator.operatorType === 'TypeCasting')
      .map(event => event.operator)
      .subscribe(op => {
        this.attribute = op.operatorProperties['attribute'];
        this.resultType = op.operatorProperties['resultType'];
        if (this.operatorID) {
          this.inputType = this.schemaPropagationService.getOperatorInputSchema(this.operatorID)
          ?.filter(e => e.attributeName === this.attribute).map(e => e.attributeType)[0];
        }
      });
  }

  ngOnInit(): void {
    console.log('ngoninit');
  }

  // invoke upon every time the input binding is changed
  ngOnChanges(): void {
    console.log('ngonchanges');
    if (! this.operatorID) {
      this.showTypeCastingTypeInformation = false;
    } else {
      this.showTypeCastingTypeInformation =
        this.workflowActionService.getTexeraGraph().getOperator(this.operatorID).operatorType === 'TypeCasting';
    }
  }


}

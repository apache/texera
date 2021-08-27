import { Component, Input, OnChanges, OnDestroy, OnInit } from '@angular/core';
import { WorkflowActionService } from 'src/app/workspace/service/workflow-graph/model/workflow-action.service';
import {
  SchemaAttribute,
  SchemaPropagationService
} from 'src/app/workspace/service/dynamic-schema/schema-propagation/schema-propagation.service';
import { OperatorPredicate } from 'src/app/workspace/types/workflow-common.interface';
import { Subscription } from 'rxjs';

// correspond to operator type specified in backend OperatorDescriptor
export const TYPE_CASTING_OPERATOR_TYPE = 'TypeCasting';

@Component({
  selector: 'texera-type-casting-display',
  templateUrl: './type-casting-display.component.html',
  styleUrls: ['./type-casting-display.component.scss']
})
export class TypeCastingDisplayComponent implements OnInit, OnDestroy, OnChanges {

  @Input() currentOperatorId: string | undefined;

  schemaToDisplay: Partial<SchemaAttribute>[] = [];
  columnNamesToDisplay: string[] = ['attributeName', 'attributeType'];
  displayTypeCastingSchemaInformation: boolean = false;
  subscriptions = new Subscription();

  constructor(
    private workflowActionService: WorkflowActionService,
    private schemaPropagationService: SchemaPropagationService,
  ) { }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

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
    this.subscriptions.add(this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
      .filter(op => op.operator.operatorID === this.currentOperatorId)
      .filter(op => op.operator.operatorType === TYPE_CASTING_OPERATOR_TYPE)
      .map(event => event.operator)
      .subscribe(op => {
        this.updateComponent(op);
      }));
  }

  updateComponent(op: OperatorPredicate): void {

    if (!this.currentOperatorId) {
      return;
    }
    this.schemaToDisplay = [];
    const inputSchema = this.schemaPropagationService.getOperatorInputSchema(this.currentOperatorId);

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




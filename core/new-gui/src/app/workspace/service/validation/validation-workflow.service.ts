import { BehaviorSubject, merge, Observable, Subject } from "rxjs";
import { Injectable } from "@angular/core";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import Ajv from "ajv";
import { filter, map } from "rxjs/operators";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";

export type ValidationError = {
  isValid: false;
  messages: Record<string, string>;
};
export type Validation = { isValid: true } | ValidationError;

/**
 *  ValidationWorkflowService handles the logic to check whether the operator is valid
 *    1. When the user add/delete operators/links
 *    2. When the user complete/delete operator properties
 *    3. When the operator ports are all connected
 *
 *  The operator will become valid if all the ports are connected and its required properties
 *    are completed by the users.
 *
 *  AJV is a javascript library that is used to validate a data object against a structure defined
 *    using a JSON Schema.
 *
 * @author Angela Wang
 */
@Injectable({
  providedIn: "root",
})
export class ValidationWorkflowService {
  public static readonly VALIDATION_OPERATOR_INPUT_MESSAGE = "inputs";
  public static readonly VALIDATION_OPERATOR_OUTPUT_MESSAGE = "outputs";

  private operatorSchemaList: ReadonlyArray<OperatorSchema> = [];
  // stream of an individual's validation status is updated, whether it's validation sucess or validation error
  private readonly operatorValidationStream = new Subject<{
    operatorID: string;
    validation: Validation;
  }>();
  // stream of global validation error status is updated, only errors will be reported
  private readonly workflowValidationErrorStream = new BehaviorSubject<{
    errors: Record<string, ValidationError>;
  }>({ errors: {} });
  private ajv = new Ajv({ allErrors: true, strict: false });

  // this map record --> <operatorID, error string>
  private workflowErrors: Record<string, ValidationError> = {};

  /**
   * subcribe the add opertor event, delete operator event, add link event, delete link event
   * and change operator property event. observe each change and record changes in operatorValidationStream
   * @param texeraGraph
   * @param workflowActionService
   */
  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService
  ) {
    // fetch operator schema list
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(filter(metadata => metadata.operators.length > 0))
      .subscribe(metadata => {
        this.operatorSchemaList = metadata.operators;
        this.initializeValidation();
      });
  }

  public getCurrentWorkflowValidationError(): {
    errors: Record<string, ValidationError>;
  } {
    return this.workflowValidationErrorStream.getValue();
  }

  /**
   * Gets observable for operatorErrorMap change event
   *
   * map: a Map<operatorID, [operatorType, error_string]
   */
  public getWorkflowValidationErrorStream(): Observable<{
    errors: Record<string, ValidationError>;
  }> {
    return this.workflowValidationErrorStream.asObservable();
  }

  /**
   * Gets the observable for operator validation change event.
   * Contains a boolean variable and an operator ID:
   *  - status: the new status for the validation of operator
   *  - operatorID: operator being validated
   */
  public getOperatorValidationStream(): Observable<{
    operatorID: string;
    validation: Validation;
  }> {
    return this.operatorValidationStream.asObservable();
  }

  public validateOperator(operatorID: string): Validation {
    if (this.workflowActionService.getTexeraGraph().isOperatorDisabled(operatorID)) {
      return { isValid: true };
    }
    const jsonSchemaValidation = this.validateJsonSchema(operatorID);
    const operatorConnectionValidation = this.validateOperatorConnection(operatorID);
    return ValidationWorkflowService.combineValidation(jsonSchemaValidation, operatorConnectionValidation);
  }

  private updateValidationState(operatorID: string, validation: Validation) {
    this.operatorValidationStream.next({ validation, operatorID });
    if (!validation.isValid) {
      this.workflowErrors[operatorID] = validation;
    } else {
      delete this.workflowErrors[operatorID];
      this.workflowValidationErrorStream.next({ errors: this.workflowErrors });
    }
    this.workflowValidationErrorStream.next({ errors: this.workflowErrors });
  }

  private updateValidationStateOnDelete(operatorID: string) {
    delete this.workflowErrors[operatorID];
    this.workflowValidationErrorStream.next({ errors: this.workflowErrors });
  }

  /**
   * Initialize all the event listener for validation on the workflow editor
   */
  private initializeValidation(): void {
    // when initialized, first validate any initial operators existing in the editor before the event handlers
    //  have been configured. This will happen when the saved workflow reload on the browser
    this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .forEach(operator => {
        this.updateValidationState(operator.operatorID, this.validateOperator(operator.operatorID));
      });

    // Capture operator dynamic schema changed event
    // dynamic schema changed event is also triggered when an operator is newly added
    this.dynamicSchemaService
      .getOperatorDynamicSchemaChangedStream()
      .subscribe(op => this.updateValidationState(op.operatorID, this.validateOperator(op.operatorID)));

    // Capture the operator delete event but not validate the deleted operator
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDeleteStream()
      .subscribe(operator => this.updateValidationStateOnDelete(operator.deletedOperatorID));

    // Capture the link add and delete event and validate the source and target operators of this link
    merge(
      this.workflowActionService.getTexeraGraph().getLinkAddStream(),
      this.workflowActionService
        .getTexeraGraph()
        .getLinkDeleteStream()
        .pipe(map(link => link.deletedLink))
    ).subscribe(link => {
      if (this.workflowActionService.getTexeraGraph().hasOperator(link.source.operatorID)) {
        this.updateValidationState(link.source.operatorID, this.validateOperator(link.source.operatorID));
      }
      if (this.workflowActionService.getTexeraGraph().hasOperator(link.target.operatorID)) {
        this.updateValidationState(link.target.operatorID, this.validateOperator(link.target.operatorID));
      }
    });

    // capture the port change event and validate the operator of this port
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPortChangeStream()
      .subscribe(portChange => {
        this.updateValidationState(
          portChange.newOperator.operatorID,
          this.validateOperator(portChange.newOperator.operatorID)
        );
      });

    // Capture the operator property change event and validate the current operator being changed
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorPropertyChangeStream()
      .subscribe(value =>
        this.updateValidationState(value.operator.operatorID, this.validateOperator(value.operator.operatorID))
      );

    // on enable / disable operator - re-validate the changed operators
    this.workflowActionService
      .getTexeraGraph()
      .getDisabledOperatorsChangedStream()
      .subscribe(event => {
        const operatorsToRevalidate = new Set<string>();

        // for every changed operator:
        event.newDisabled.concat(event.newEnabled).forEach(op => {
          // revalidate itself
          operatorsToRevalidate.add(op);

          // revalidate all its input operators
          const inputs = this.workflowActionService.getTexeraGraph().getInputLinksByOperatorId(op);
          inputs.forEach(link => operatorsToRevalidate.add(link.source.operatorID));

          // revliadate all its output operators
          const outputs = this.workflowActionService.getTexeraGraph().getOutputLinksByOperatorId(op);
          outputs.forEach(link => operatorsToRevalidate.add(link.target.operatorID));
        });

        operatorsToRevalidate.forEach(op => this.updateValidationState(op, this.validateOperator(op)));
      });
  }

  /**
   * This method is used to check whether all required properties of the operator have been completed.
   *  If completed correctly, the operator is valid.
   */
  private validateJsonSchema(operatorID: string): Validation {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (operator === undefined) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }

    // try to fetch dynamic schema first
    const operatorSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);
    if (operatorSchema === undefined) {
      throw new Error("operatorSchema doesn't exist");
    }

    const isValid = this.ajv.validate(operatorSchema.jsonSchema, operator.operatorProperties);
    if (isValid) {
      return { isValid: true };
    }

    const errors = this.ajv.errors;
    const validationError: Record<string, string> = {};
    if (errors) {
      errors.forEach(error => (validationError[error.keyword] = error.message ? error.message : ""));
    }
    return { isValid: false, messages: validationError };
  }

  /**
   * This method is used to check whether all ports of the operator have been connected.
   *  if all ports of the operator are connected, the operator is valid.
   */
  private validateOperatorConnection(operatorID: string): Validation {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (operator === undefined) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    if (operatorSchema === undefined) {
      throw new Error("operatorSchema doesn't exist");
    }

    const texeraGraph = this.workflowActionService.getTexeraGraph();

    // check if input links satisfy the requirement
    const numInputLinksByPort = new Map<string, number>();
    texeraGraph.getInputLinksByOperatorId(operatorID).forEach(inLink => {
      if (texeraGraph.isLinkEnabled(inLink.linkID)) {
        const portID = inLink.target.portID;
        const num = numInputLinksByPort.get(portID) ?? 0;
        numInputLinksByPort.set(portID, num + 1);
      }
    });

    let satisfyInput = true;
    let inputPortsViolationMessage = "";
    for (let i = 0; i < operator.inputPorts.length; i++) {
      const port = operator.inputPorts[i];
      const portNumInputs = numInputLinksByPort.get(port.portID) ?? 0;
      if (port.allowMultiInputs) {
        if (portNumInputs < 1) {
          satisfyInput = false;
          inputPortsViolationMessage += `${port.displayName ?? ""} requires at least 1 inputs, has ${portNumInputs}`;
        }
      } else {
        if (portNumInputs !== 1) {
          satisfyInput = false;
          inputPortsViolationMessage += `${port.displayName ?? ""} requires 1 input, has ${portNumInputs}`;
        }
      }
    }

    // check if output links satisfy the requirement
    const requiredOutputNum = operator.outputPorts.length;
    const actualOutputNum = texeraGraph
      .getOutputLinksByOperatorId(operatorID)
      .filter(link => texeraGraph.isLinkEnabled(link.linkID)).length;

    // If the operator is the sink operator, the actual output number must be equal to required number.
    const satisyOutput =
      this.operatorMetadataService.getOperatorSchema(operator.operatorType).additionalMetadata.operatorGroupName ===
      "View Results"
        ? requiredOutputNum === actualOutputNum
        : requiredOutputNum <= actualOutputNum;

    const outputPortsViolationMessage = satisyOutput
      ? ""
      : `requires ${requiredOutputNum} outputs, has ${actualOutputNum} outputs`;

    if (satisfyInput && satisyOutput) {
      return { isValid: true };
    } else {
      const messages: Record<string, string> = {};
      if (!satisfyInput) {
        messages[ValidationWorkflowService.VALIDATION_OPERATOR_INPUT_MESSAGE] = inputPortsViolationMessage;
      }
      if (!satisyOutput) {
        messages[ValidationWorkflowService.VALIDATION_OPERATOR_OUTPUT_MESSAGE] = outputPortsViolationMessage;
      }
      return { isValid: false, messages: messages };
    }
  }

  public static combineValidation(...validations: Validation[]): Validation {
    let isValid = true;
    let messages = {};
    validations.forEach(validation => {
      isValid = isValid && validation.isValid;
      if (!validation.isValid) {
        messages = { ...messages, ...validation.messages };
      }
    });
    if (isValid) {
      return { isValid };
    } else {
      return { isValid, messages };
    }
  }
}

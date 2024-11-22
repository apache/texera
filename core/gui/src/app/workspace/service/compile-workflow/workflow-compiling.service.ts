import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { EMPTY, merge, Observable, ReplaySubject } from "rxjs";
import { CustomJSONSchema7 } from "src/app/workspace/types/custom-json-schema.interface";
import { AppSettings } from "../../../common/app-setting";
import { areOperatorSchemasEqual, OperatorSchema } from "../../types/operator-schema.interface";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { catchError, debounceTime, mergeMap } from "rxjs/operators";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import {
  AttributeType,
  CompilationState,
  CompilationStateInfo,
  OperatorInputSchema,
  PortInputSchema,
  WorkflowCompilationResponse,
} from "../../types/workflow-compiling.interface";
import { WorkflowFatalError } from "../../types/workflow-websocket.interface";
import { LogicalPlan } from "../../types/execute-workflow.interface";

// endpoint for workflow compile
export const WORKFLOW_COMPILATION_ENDPOINT = "compile";

export const WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS = 500;

/**
 * Workflow Compiling Service provides mainly 3 functionalities:
 * 1. autocomplete attribute property of operators (previously done by the SchemaPropagationService)
 * 2. receive static errors (previously done by sending EditingTimeCompilationRequest and saving in the ExecutionStateInfo)
 * 3. manage PhysicalPlan (TODO: send the physical plan to the standalone WorkflowExecutingService once we have it)
 *
 * When user creates and connects operators in workflow, the WorkflowCompilingService's api will be triggered, which,
 * propagate the schemas, compiles the user's workflow to get the physical plan and static errors(if any).
 *
 * Specifically for schema autocomplete, by contract, property name `attribute` and `attributes` indicate the field is a column of the operator's input,
 *  and schema propagation can provide autocomplete for the column names.
 */
@Injectable({
  providedIn: "root",
})
export class WorkflowCompilingService {
  private currentCompilationStateInfo: CompilationStateInfo = {
    state: CompilationState.Uninitialized,
  };
  private compilationStateInfoChangedStream = new ReplaySubject<CompilationState>(1);

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService
  ) {
    // invoke the compilation service when there are any changes on workflow topology and properties. This includes:
    // - operator add, delete, property changed, disabled
    // - link add, delete
    merge(
      this.workflowActionService.getTexeraGraph().getLinkAddStream(),
      this.workflowActionService.getTexeraGraph().getLinkDeleteStream(),
      this.workflowActionService.getTexeraGraph().getOperatorAddStream(),
      this.workflowActionService.getTexeraGraph().getOperatorDeleteStream(),
      this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream(),
      this.workflowActionService.getTexeraGraph().getDisabledOperatorsChangedStream()
    )
      .pipe(debounceTime(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS))
      .pipe(
        mergeMap(() =>
          this.compile(ExecuteWorkflowService.getLogicalPlanRequest(this.workflowActionService.getTexeraGraph()))
        )
      )
      .subscribe(response => {
        if (response.physicalPlan) {
          this.currentCompilationStateInfo = {
            state: CompilationState.Succeeded,
            physicalPlan: response.physicalPlan,
            operatorInputSchemaMap: response.operatorInputSchemas,
          };
        } else {
          this.currentCompilationStateInfo = {
            state: CompilationState.Failed,
            operatorInputSchemaMap: response.operatorInputSchemas,
            operatorErrors: response.operatorErrors,
          };
        }
        this.compilationStateInfoChangedStream.next(this.currentCompilationStateInfo.state);
        this._applySchemaPropagationResult(this.currentCompilationStateInfo.operatorInputSchemaMap);
      });
  }

  public getWorkflowCompilationState(): CompilationState {
    return this.currentCompilationStateInfo.state;
  }

  public getWorkflowCompilationErrors(): Readonly<Record<string, WorkflowFatalError>> {
    if (
      this.currentCompilationStateInfo.state === CompilationState.Succeeded ||
      this.currentCompilationStateInfo.state === CompilationState.Uninitialized
    ) {
      return {};
    }
    return this.currentCompilationStateInfo.operatorErrors;
  }

  public getOperatorInputSchema(operatorID: string): OperatorInputSchema | undefined {
    if (this.currentCompilationStateInfo.state == CompilationState.Uninitialized) {
      return undefined;
    }
    return this.currentCompilationStateInfo.operatorInputSchemaMap[operatorID];
  }

  public getPortInputSchema(operatorID: string, portIndex: number): PortInputSchema | undefined {
    return this.getOperatorInputSchema(operatorID)?.[portIndex];
  }

  public getOperatorInputAttributeType(
    operatorID: string,
    portIndex: number,
    attributeName: string
  ): AttributeType | undefined {
    return this.getPortInputSchema(operatorID, portIndex)?.find(e => e.attributeName === attributeName)?.attributeType;
  }

  /**
   * Apply the schema propagation result to an operator.
   * The schema propagation result contains the input attributes of operators.
   *
   * If an operator is not in the result, then:
   * 1. the operator's input attributes cannot be inferred. In this case, the operator dynamic schema is unchanged.
   * 2. the operator is a source operator. In this case, we need to fill in the attributes using the selected table.
   *
   * @param schemaPropagationResult
   */
  private _applySchemaPropagationResult(schemaPropagationResult: { [key: string]: OperatorInputSchema }): void {
    // for each operator, try to apply schema propagation result
    Array.from(this.dynamicSchemaService.getDynamicSchemaMap().keys()).forEach(operatorID => {
      const currentDynamicSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);

      // if operator input attributes are in the result, set them in dynamic schema
      let newDynamicSchema: OperatorSchema;
      if (schemaPropagationResult[operatorID]) {
        newDynamicSchema = WorkflowCompilingService.setOperatorInputAttrs(
          currentDynamicSchema,
          schemaPropagationResult[operatorID]
        );
      } else {
        // otherwise, the input attributes of the operator is unknown
        // if the operator is not a source operator, restore its original schema of input attributes
        if (currentDynamicSchema.additionalMetadata.inputPorts.length > 0) {
          newDynamicSchema = WorkflowCompilingService.restoreOperatorInputAttrs(currentDynamicSchema);
        } else {
          newDynamicSchema = currentDynamicSchema;
        }
      }

      if (!areOperatorSchemasEqual(currentDynamicSchema, newDynamicSchema)) {
        this.dynamicSchemaService.setDynamicSchema(operatorID, newDynamicSchema);
      }
    });
  }

  /**
   * Used for automated propagation of input schema in workflow.
   *
   * When users are in the process of building a workflow, Texera can propagate schema forwards so
   * that users can easily set the properties of the next operator. For eg: If there are two operators Source:Scan and KeywordSearch and
   * a link is created between them, the attributed of the table selected in Source can be propagated to the KeywordSearch operator.
   */
  private compile(logicalPlan: LogicalPlan): Observable<WorkflowCompilationResponse> {
    // create a Logical Plan based on the workflow graph
    // remove unnecessary information for schema propagation.
    const body = {
      operators: logicalPlan.operators,
      links: logicalPlan.links,
      opsToReuseResult: [],
      opsToViewResult: [],
    };
    // make a http post request to the API endpoint with the logical plan object
    return this.httpClient
      .post<WorkflowCompilationResponse>(
        `${AppSettings.getApiEndpoint()}/${WORKFLOW_COMPILATION_ENDPOINT}`,
        JSON.stringify(body),
        {
          headers: new HttpHeaders({
            "Content-Type": "application/json",
          }),
        }
      )
      .pipe(
        catchError((err: unknown) => {
          console.warn("compile workflow API returns error", err);
          return EMPTY;
        })
      );
  }

  public static setOperatorInputAttrs(
    operatorSchema: OperatorSchema,
    inputAttributes: OperatorInputSchema | undefined
  ): OperatorSchema {
    // If the inputSchema is empty, just return the original operator metadata.
    if (!inputAttributes || inputAttributes.length === 0) {
      return operatorSchema;
    }

    let newJsonSchema = operatorSchema.jsonSchema;

    const getAttrNames = (attrName: string, v: CustomJSONSchema7): string[] | undefined => {
      const i = v.autofillAttributeOnPort;
      if (i === undefined || i === null || !Number.isInteger(i) || i >= inputAttributes.length) {
        return undefined;
      }
      const inputAttrAtPort = inputAttributes[i];
      if (!inputAttrAtPort) {
        return undefined;
      }
      const attrNames: string[] = inputAttrAtPort.map(attr => attr.attributeName);
      if (v.additionalEnumValue) {
        attrNames.push(v.additionalEnumValue);
      }

      // ajv does not support null values, so it converts all the nulls to empty strings.
      // https://github.com/ajv-validator/ajv/issues/1471
      // the null -> "" change is done by Ajv.validate() with useDefault set to true.
      // It is converted during the property editor form initialization and workflow validation, instead of during schema propagation.
      if (!operatorSchema.jsonSchema.required?.includes(attrName)) {
        if (v.default) {
          if (typeof v.default !== "string") {
            throw new Error("default value must be a string");
          }
          // We are adding the default value or "" into
          // the enum list to pass the frontend check for optional properties.
          attrNames.push(v.default);
        } else {
          attrNames.push("");
        }
      }
      return attrNames;
    };

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeName",
      (attrName, old) => ({
        ...old,
        type: "string",
        enum: getAttrNames(attrName, old),
        uniqueItems: true,
      })
    );

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeNameList",
      (attrName, old) => ({
        ...old,
        type: "array",
        uniqueItems: true,
        items: {
          ...(old.items as CustomJSONSchema7),
          type: "string",
          enum: getAttrNames(attrName, old),
        },
      })
    );

    return {
      ...operatorSchema,
      jsonSchema: newJsonSchema,
    };
  }

  public static restoreOperatorInputAttrs(operatorSchema: OperatorSchema): OperatorSchema {
    let newJsonSchema = operatorSchema.jsonSchema;

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeName",
      (attrName, old) => ({
        ...old,
        type: "string",
        enum: undefined,
        uniqueItems: undefined,
      })
    );

    newJsonSchema = DynamicSchemaService.mutateProperty(
      newJsonSchema,
      (k, v) => v.autofill === "attributeNameList",
      (attrName, old) => ({
        ...old,
        type: "array",
        uniqueItems: undefined,
        items: {
          ...(old.items as CustomJSONSchema7),
          type: "string",
          enum: undefined,
        },
      })
    );

    return {
      ...operatorSchema,
      jsonSchema: newJsonSchema,
    };
  }

  public getCompilationStateInfoChangedStream(): Observable<CompilationState> {
    return this.compilationStateInfoChangedStream.asObservable();
  }
}

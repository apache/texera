import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { isEqual } from "lodash-es";
import { EMPTY, merge, Observable, Subject } from "rxjs";
import { CustomJSONSchema7 } from "src/app/workspace/types/custom-json-schema.interface";
import { environment } from "../../../../environments/environment";
import { AppSettings } from "../../../common/app-setting";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { DEFAULT_WORKFLOW, WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { catchError, debounceTime, mergeMap } from "rxjs/operators";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { PhysicalPlan } from "../../../common/type/physical-plan";
import { CompilationState, CompilationStateInfo } from "../../types/compile-workflow.interface";
import { WorkflowFatalError } from "../../types/workflow-websocket.interface";
import { AuthService } from "../../../common/service/user/auth.service";

// endpoint for schema propagation
export const WORKFLOW_COMPILATION_ENDPOINT = "compilation";

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
  private compilationStateInfoChangedStream = new Subject<void>();

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService
  ) {
    // do nothing if schema propagation is not enabled
    if (!environment.schemaPropagationEnabled) {
      return;
    }

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
      .pipe(mergeMap(() => this.invokeWorkflowCompilationAPI()))
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
        console.log(this.currentCompilationStateInfo);
        this.compilationStateInfoChangedStream.next();
        this._applySchemaPropagationResult(this.currentCompilationStateInfo.operatorInputSchemaMap);
      });
  }

  public getWorkflowCompilationState(): CompilationState {
    return this.currentCompilationStateInfo.state;
  }

  public getWorkflowCompilationErrors(): Readonly<Array<WorkflowFatalError>> {
    if (
      this.currentCompilationStateInfo.state === CompilationState.Succeeded ||
      this.currentCompilationStateInfo.state === CompilationState.Uninitialized
    ) {
      return [];
    }
    return this.currentCompilationStateInfo.operatorErrors;
  }

  public getOperatorInputSchemaMap(): Readonly<Record<string, OperatorInputSchema>> {
    if (this.currentCompilationStateInfo.state == CompilationState.Uninitialized) {
      return {};
    }
    return this.currentCompilationStateInfo.operatorInputSchemaMap;
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

      if (!isEqual(currentDynamicSchema, newDynamicSchema)) {
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
  private invokeWorkflowCompilationAPI(): Observable<WorkflowCompilationResponse> {
    // create a Logical Plan based on the workflow graph
    const body = ExecuteWorkflowService.getLogicalPlanRequest(this.workflowActionService.getTexeraGraph());
    // remove unnecessary information for schema propagation.
    const body2 = {
      operators: body.operators,
      links: body.links,
      opsToReuseResult: [],
      opsToViewResult: [],
    };
    // make a http post request to the API endpoint with the logical plan object
    return this.httpClient
      .post<WorkflowCompilationResponse>(
        `${AppSettings.getTexeraApiEndpoint()}/${WORKFLOW_COMPILATION_ENDPOINT}/${
          this.workflowActionService.getWorkflow().wid ?? DEFAULT_WORKFLOW.wid
        }`,
        JSON.stringify(body2),
        {
          headers: new HttpHeaders({
            Authorization: `Bearer ${AuthService.getAccessToken()}`,
            "Content-Type": "application/json",
          }),
        }
      )
      .pipe(
        catchError((err: unknown) => {
          console.log("schema propagation API returns error", err);
          return EMPTY;
        })
      );
  }

  /**
   * This method reset the attribute / attributes fields of a operator properties
   *  when the json schema has been changed, since the attribute fields might
   *  be different for each json schema.
   *
   * For instance,
   *  twitter_sample table contains the 'country' attribute
   *  promed table does not contain the 'country' attribute
   *
   * @param workflowActionService
   * @param operatorID operator that has the changed schema
   */
  public static resetAttributeOfOperator(workflowActionService: WorkflowActionService, operatorID: string): void {
    const operator = workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (!operator) {
      throw new Error(`${operatorID} not found`);
    }

    // recursive function that removes the attribute properties and returns the new object
    const walkPropertiesRecurse = (propertyObject: { [key: string]: any }) => {
      if (propertyObject === null || propertyObject === undefined) {
        return propertyObject;
      }
      Object.keys(propertyObject).forEach(key => {
        if (key === "attribute" || key === "attributes") {
          const {
            [key]: [],
            ...removedAttributeProperties
          } = propertyObject;
          propertyObject = removedAttributeProperties;
        } else if (typeof propertyObject[key] === "object") {
          propertyObject[key] = walkPropertiesRecurse(propertyObject[key]);
        }
      });

      return propertyObject;
    };

    const propertyClone = walkPropertiesRecurse(operator.operatorProperties);
    workflowActionService.setOperatorProperty(operatorID, propertyClone);
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

  public getCompilationStateInfoChangedStream(): Observable<void> {
    return this.compilationStateInfoChangedStream.asObservable();
  }
}

// possible types of an attribute
export type AttributeType = "string" | "integer" | "double" | "boolean" | "long" | "timestamp" | "binary";

// schema: an array of attribute names and types
export interface SchemaAttribute
  extends Readonly<{
    attributeName: string;
    attributeType: AttributeType;
  }> {}

// input schema of an operator: an array of schemas at each input port
export type OperatorInputSchema = ReadonlyArray<PortInputSchema | undefined>;
export type PortInputSchema = ReadonlyArray<SchemaAttribute>;

/**
 * The backend interface of the return object of a successful/failed workflow compilation
 *
 * An example data format for AutocompleteSuccessResult will look like:
 * {
 *  physicalPlan: Physical Plan | Null(if compilation failed),
 *  operatorInputSchemas: {
 *    'operatorID1' : [ ['attribute1','attribute2','attribute3'] ],
 *    'operatorID2' : [ [ {attributeName: 'name', attributeType: 'string'},
 *                      {attributeName: 'text', attributeType: 'string'},
 *                      {attributeName: 'follower_count', attributeType: 'string'} ] ]
 *
 *  }
 * }
 */
export interface WorkflowCompilationResponse
  extends Readonly<{
    physicalPlan?: PhysicalPlan;
    operatorInputSchemas: {
      [key: string]: OperatorInputSchema;
    };
    operatorErrors: Array<WorkflowFatalError>;
  }> {}

/**
 * The backend interface of the return object of a failed execution of
 * autocomplete API
 */
export interface SchemaPropagationError
  extends Readonly<{
    code: -1;
    message: string;
  }> {}

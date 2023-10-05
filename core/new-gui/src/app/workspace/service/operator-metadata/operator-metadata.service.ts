import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { OperatorMetadata, OperatorSchema } from "../../types/operator-schema.interface";
import { BreakpointSchema } from "../../types/workflow-common.interface";
import { mockBreakpointSchema } from "./mock-operator-metadata.data";
import { shareReplay } from "rxjs/operators";

export const OPERATOR_METADATA_ENDPOINT = "resources/operator-metadata";

const addDictionaryAPIAddress = "/api/resources/dictionary/";
const getDictionaryAPIAddress = "/api/upload/dictionary/";

// interface only containing public methods
export type IOperatorMetadataService = Pick<OperatorMetadataService, keyof OperatorMetadataService>;

/**
 * OperatorMetadataService talks to the backend to fetch the operator metadata, which contains a list of operator schemas.
 * Each operator schema contains all the information related to an operator, for example, operatorType, userFriendlyName,
 *  and the jsonSchema of its properties.
 *
 * Components and Services should call getOperatorMetadata() and subscribe to the Observable to get the metadata,
 *  after the metadata is fetched from the backend, it will be broadcast through the observable.
 *
 * The mock operator metadata is also available in mock-operator-metadata.ts for testing.
 * It contains the schemas for 3 operators.
 * @author Zuozhi Wang
 *
 */
@Injectable({
  providedIn: "root",
})
export class OperatorMetadataService {
  // holds the current version of operator metadata
  private currentOperatorMetadata: OperatorMetadata | undefined;
  private readonly currentBreakpointSchema: BreakpointSchema | undefined;

  private operatorMetadataObservable = this.httpClient
    .get<OperatorMetadata>(`${AppSettings.getApiEndpoint()}/${OPERATOR_METADATA_ENDPOINT}`)
    .pipe(shareReplay(1));

  constructor(private httpClient: HttpClient) {
    this.getOperatorMetadata().subscribe(data => {
      this.currentOperatorMetadata = data;
    });
    // At current design, all the links have one fixed breakpoint schema stored in the frontend
    this.currentBreakpointSchema = mockBreakpointSchema;
  }

  /**
   * Gets an Observable for operator metadata.
   * This observable will emit OperatorMetadataValue after the data is fetched from the backend.
   *
   * // TODO: refactor this to 2 functions: getOperatorMetadataStream() and getOperatorMetadata()
   */
  public getOperatorMetadata(): Observable<OperatorMetadata> {
    return this.operatorMetadataObservable;
  }

  public getOperatorSchema(operatorType: string): OperatorSchema {
    if (!this.currentOperatorMetadata) {
      throw new Error("operator metadata is undefined");
    }
    const operatorSchema = this.currentOperatorMetadata.operators.find(schema => schema.operatorType === operatorType);
    if (!operatorSchema) {
      throw new Error(`can\'t find operator schema of type ${operatorType}`);
    }
    return operatorSchema;
  }

  /**
   * Returns true if the operator type exists *in the current operator metadata*.
   * For example, if the first HTTP request to the backend hasn't returned yet,
   *  the current operator metadata is empty, and no operator type exists.
   *
   * @param operatorType - Operator name string that we are checking for existence *in the current operator metadata*
   * @param userFriendlyNameFilter - If true, checks if operatorType matches an operator's user friendly or type name
   * @param caseInsensitive - If true, operatorType checking becomes case insensitive
   */
  public operatorTypeExists(
    operatorType: string,
    userFriendlyNameFilter: boolean = false,
    caseInsensitive: boolean = false
  ): boolean {
    if (!this.currentOperatorMetadata) {
      return false;
    }
    const operator = this.currentOperatorMetadata.operators.filter(op => {
      let operatorTypeInMetadata = op.operatorType;
      let operatorNameInMetadata = op.additionalMetadata.userFriendlyName;
      if (caseInsensitive) {
        operatorTypeInMetadata = operatorTypeInMetadata.toLowerCase();
        operatorNameInMetadata = operatorNameInMetadata.toLowerCase();
        operatorType = operatorType.toLowerCase();
      }
      if (userFriendlyNameFilter) {
        return operatorTypeInMetadata === operatorType || operatorNameInMetadata === operatorType;
      } else {
        return operatorTypeInMetadata === operatorType;
      }
    });
    if (operator.length === 0) {
      return false;
    }
    return true;
  }

  /**
   * At current design, this function returns the fixed schema
   */
  public getBreakpointSchema(): BreakpointSchema {
    if (!this.currentBreakpointSchema) {
      throw new Error("breakpoint schema is undefined");
    }
    return this.currentBreakpointSchema;
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { mockOperatorMetaData } from "./mock-operator-metadata.data";
import { OperatorMetadata, OperatorSchema } from "../../types/operator-schema.interface";
import { IOperatorMetadataService } from "./operator-metadata.service";
import { shareReplay } from "rxjs/operators";

@Injectable()
export class StubOperatorMetadataService implements IOperatorMetadataService {
  private operatorMetadataObservable = of(mockOperatorMetaData).pipe(shareReplay(1));

  public getOperatorSchema(operatorType: string): OperatorSchema {
    const operatorSchema = mockOperatorMetaData.operators.find(schema => schema.operatorType === operatorType);
    if (!operatorSchema) {
      throw new Error(`can\'t find operator schema of type ${operatorType}`);
    }
    return operatorSchema;
  }

  public getOperatorMetadata(): Observable<OperatorMetadata> {
    return this.operatorMetadataObservable;
  }

  public operatorTypeExists(operatorType: string): boolean {
    const operator = mockOperatorMetaData.operators.filter(op => op.operatorType === operatorType);
    if (operator.length === 0) {
      return false;
    }
    return true;
  }
}

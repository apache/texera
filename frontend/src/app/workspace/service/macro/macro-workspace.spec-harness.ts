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
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { HttpClient } from "@angular/common/http";
import { throwError } from "rxjs";
import { MacroExpansionService } from "./macro-expansion.service";

/** Unconfigured previews fail promptly while other services keep their own HTTP test backend. */
export const macroWorkspaceTestProviders = [
  ...commonTestProviders,
  {
    provide: MacroExpansionService,
    useFactory: () =>
      new MacroExpansionService(
        new HttpClient({
          handle: () => throwError(() => new Error("No backend expansion response configured")),
        })
      ),
  },
  { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
];

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
import { HttpClient } from "@angular/common/http";
import { Observable, catchError, map, of } from "rxjs";
import { OperatorLink, OperatorPredicate, Point } from "../../types/workflow-common.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";

/**
 * A single operator suggestion returned by the agent-service `/api/recommend`
 * endpoint (apache/texera#5240). Mirrors the backend `OperatorRecommendation`.
 */
export interface OperatorRecommendation {
  /** Recommended operator type (validated against the live catalog when available). */
  operatorType: string;
  /** Confidence in `[0, 1]`, monotonically non-increasing down the list. */
  score: number;
  /** Short, human-readable rationale shown alongside the suggested operator. */
  reason: string;
  /** Display name from operator metadata, when available. */
  userFriendlyName?: string;
}

interface RecommendationResponse {
  recommendations: OperatorRecommendation[];
  strategy: "hardcoded" | "llm";
}

/**
 * Client for the ambient operator recommender. Asks the stateless agent-service
 * endpoint what operators are likely to follow the one just added, and turns a
 * chosen suggestion into a real operator wired onto the source's output port.
 *
 * The service never fails loudly: the recommender is a non-essential, ambient
 * aid, so a backend error or a disabled feature simply yields no suggestions
 * and the canvas behaves exactly as before.
 */
@Injectable({
  providedIn: "root",
})
export class OperatorRecommendationService {
  private static readonly RECOMMEND_API_URL = "/api/recommend";

  // Horizontal gap between the source operator and a materialized suggestion.
  private static readonly MATERIALIZE_GAP_X = 100;

  constructor(
    private http: HttpClient,
    private config: GuiConfigService,
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService
  ) {}

  /** Whether the opt-in recommender feature is turned on for this deployment. */
  public isEnabled(): boolean {
    return this.config.env.operatorRecommendationEnabled === true;
  }

  /**
   * Fetch ranked next-operator suggestions for the operator just added.
   *
   * Returns an empty list (never errors) when the feature is disabled, the
   * operator has no output port to suggest from, or the backend call fails.
   *
   * How many suggestions come back is the backend's call: it defaults to three and
   * clamps any larger requested limit, so there is nothing useful to send from here.
   *
   * @param operator the operator that was just added to the canvas
   */
  public getRecommendations(operator: OperatorPredicate): Observable<OperatorRecommendation[]> {
    // An operator with no output ports (e.g. a chart sink) has no port to anchor
    // suggestions on, so we skip the backend call.
    if (!this.isEnabled() || operator.outputPorts.length === 0) {
      return of([]);
    }

    const existingOperatorTypes = this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .map(op => op.operatorType);

    return this.http
      .post<RecommendationResponse>(OperatorRecommendationService.RECOMMEND_API_URL, {
        operatorType: operator.operatorType,
        existingOperatorTypes,
      })
      .pipe(
        map(response => response.recommendations ?? []),
        // Ambient feature: swallow failures and fall back to "no suggestions".
        catchError(() => of([]))
      );
  }

  /**
   * Turn a chosen suggestion into a real operator: create it just to the right
   * of the source operator and link the source's output port to the new
   * operator's first input port. Added as a single undoable action.
   *
   * The backend only validates suggestions against the operator catalog when its
   * own catalog is initialized, so an unknown `recommendedType` can reach us and
   * make `getNewOperatorPredicate` throw. Materializing is a click on an ambient
   * hint, not an action the user asked to be told about, so a failure leaves the
   * canvas untouched and returns `undefined` rather than surfacing an error.
   *
   * @param sourceOperator the operator the suggestion was made from
   * @param sourceOutputPortID the output port the suggestion was anchored on
   * @param recommendedType the operator type the user clicked
   * @returns the new operator's ID, or `undefined` if it could not be created
   */
  public materialize(
    sourceOperator: OperatorPredicate,
    sourceOutputPortID: string,
    recommendedType: string
  ): string | undefined {
    try {
      const newOperator = this.workflowUtilService.getNewOperatorPredicate(recommendedType);

      const sourcePosition = this.workflowActionService
        .getJointGraphWrapper()
        .getElementPosition(sourceOperator.operatorID);
      const newPosition: Point = {
        x: sourcePosition.x + JointUIService.DEFAULT_OPERATOR_WIDTH + OperatorRecommendationService.MATERIALIZE_GAP_X,
        y: sourcePosition.y,
      };

      const links: OperatorLink[] = [];
      // Every recommended successor has an input port, but guard defensively so a
      // stale rule can never throw on the canvas.
      if (newOperator.inputPorts.length > 0) {
        links.push({
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: sourceOperator.operatorID, portID: sourceOutputPortID },
          target: { operatorID: newOperator.operatorID, portID: newOperator.inputPorts[0].portID },
        });
      }

      this.workflowActionService.addOperatorsAndLinks([{ op: newOperator, pos: newPosition }], links);
      return newOperator.operatorID;
    } catch (error) {
      // Fail soft, but leave a breadcrumb: silently doing nothing on click is
      // otherwise indistinguishable from the feature being broken.
      console.warn(`Unable to materialize recommended operator "${recommendedType}".`, error);
      return undefined;
    }
  }
}

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

import { UndoRedoService } from "../../undo-redo/undo-redo.service";
import { SyncTexeraModel } from "./sync-texera-model";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { WorkflowGraph } from "./workflow-graph";
import { OperatorLink } from "../../../types/workflow-common.interface";
import {
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
} from "./mock-workflow-data";
import { TestBed } from "@angular/core/testing";
import { marbles } from "rxjs-marbles";
import * as joint from "jointjs";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("SyncTexeraModel", () => {
  let texeraGraph: WorkflowGraph;
  let jointGraph: joint.dia.Graph;
  let jointGraphWrapper: JointGraphWrapper;

  /**
   * Returns a mock JointJS Link object (joint.dia.Link)
   * It includes the attributes and functions same as JointJS
   * @param link
   */
  function getJointLinkValue(link: OperatorLink): joint.dia.Link {
    return {
      id: link.linkID,
      attributes: {
        source: { id: link.source.operatorID, port: link.source.portID },
        target: { id: link.target.operatorID, port: link.target.portID },
      },
    } as any as joint.dia.Link;
  }

  /**
   * This helper function returns a mock JointJS link object (joint.dia.Link)
   *  that is only connected to a source port, but detached from the target port.
   *
   * This scenario happens when the user is still moving the link
   *  and it is not connected to a target port.
   *
   * @param link an operator link, but the target operator and target link is ignored
   */
  function getIncompleteJointLink(link: OperatorLink): joint.dia.Link {
    return {
      id: link.linkID,
      getSourceElement: () => ({ id: link.source.operatorID }),
      getTargetElement: () => null,
      get: (port: string) => {
        if (port === "source") {
          return { port: link.source.portID };
        } else if (port === "target") {
          return null;
        } else {
          throw new Error("getJointLinkValue: mock is inconsistent with implementation");
        }
      },
    } as joint.dia.Link;
  }

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UndoRedoService,
        WorkflowUtilService,
        JointUIService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    });

    texeraGraph = new WorkflowGraph();
    jointGraph = new joint.dia.Graph();
    jointGraphWrapper = new JointGraphWrapper(jointGraph);
  });

  /**
   * Test JointJS add link `getJointLinkCellAddStream` event stream handled properly
   *
   * Add two operators
   * Then emit one add link event from JointJS
   *
   * addOperator
   * jointAddLink:  -----p-|
   *
   * Expected:
   * The graph should have two operators and a link between the operators
   */
  it(
    "should add a link when link add event happen from JointJS",
    marbles(m => {
      // add operators
      texeraGraph.addOperator(mockScanPredicate);
      texeraGraph.addOperator(mockResultPredicate);

      // prepare add link
      const addLinkMarbleString = "-----p-|";
      const addLinkMarbleValues = {
        p: getJointLinkValue(mockScanResultLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellAddStream").mockReturnValue(
        m.hot(addLinkMarbleString, addLinkMarbleValues)
      );

      // construct the texera sync model with spied dependencies
      const syncTexeraModel = new SyncTexeraModel(texeraGraph, jointGraphWrapper);

      jointGraphWrapper.getJointLinkCellAddStream().subscribe({
        complete: () => {
          expect(texeraGraph.getAllOperators().length).toEqual(2);
          expect(texeraGraph.getAllLinks().length).toEqual(1);
          expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeTruthy();
          expect(texeraGraph.getLinkWithID(mockScanResultLink.linkID)).toEqual(mockScanResultLink);
          expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeTruthy();
        },
      });
    })
  );

  /**
   * Test JointJS add link `getJointLinkCellAddStream` event stream handled properly
   *  when the added JointJS link is invalid.
   *
   * Add two operators
   * Then a user drags a link from a source port,
   *  the link is visually added,
   *  but the link is not yet connected to a target port.
   * This link is considered invalid and should not appear in the graph
   *
   * addOperator
   * jointAddLink:  -----q-| (q is an incomplete Joint link)
   *
   * Expected:
   * The graph doesn't contain the incomplete link
   */
  it(
    "should not create a link when an incomplete link is added in JointJS",
    marbles(m => {
      // add operators
      texeraGraph.addOperator(mockScanPredicate);
      texeraGraph.addOperator(mockResultPredicate);

      // prepare add link (incomplete link)
      const addLinkMarbleString = "-----q-|";
      const addLinkMarbleValues = {
        q: getIncompleteJointLink(mockScanResultLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellAddStream").mockReturnValue(
        m.hot(addLinkMarbleString, addLinkMarbleValues)
      );

      // construct the texera sync model with spied dependencies
      const syncTexeraModel = new SyncTexeraModel(texeraGraph, jointGraphWrapper);

      jointGraphWrapper.getJointLinkCellDeleteStream().subscribe({
        complete: () => {
          expect(texeraGraph.getAllLinks().length).toEqual(0);
        },
      });
    })
  );

  /**
   * Test JointJS delete link `getJointLinkCellDeleteStream` event stream handled properly
   *
   * Add two operators and one link
   * Then emit one delete link event from JointJS
   *
   * add operators + links: 1 -> 2
   * jointDeleteLink: -------r-|
   *
   * Expected:
   * The link should be deleted
   */
  it(
    "should delete a link when link delete event happens from JointJS",
    marbles(m => {
      // add operators
      texeraGraph.addOperator(mockScanPredicate);
      texeraGraph.addOperator(mockResultPredicate);

      // add links
      texeraGraph.addLink(mockScanResultLink);

      // prepare delete link
      const deleteLinkMarbleString = "-------r-|";
      const deleteLinkMarbleValues = {
        r: getJointLinkValue(mockScanResultLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellDeleteStream").mockReturnValue(
        m.hot(deleteLinkMarbleString, deleteLinkMarbleValues)
      );

      // construct the texera sync model with spied dependencies
      const syncTexeraModel = new SyncTexeraModel(texeraGraph, jointGraphWrapper);

      jointGraphWrapper.getJointLinkCellDeleteStream().subscribe({
        complete: () => {
          expect(texeraGraph.getAllLinks().length).toEqual(0);
        },
      });
    })
  );

  /**
   * Test JointJS delete link `getJointLinkCellDeleteStream` event stream handled properly,
   *  when the deleted link is invalid and never existed in texera graph.
   *
   * Add two operators
   * Then a user drags a link from a source port,
   *  the link is visually added,
   *  but the link is not yet connected to a target port.
   * Then the user release the mouse and the link is visually deleted,
   *  JointJS emits Link Delete event,
   *  the workflow graph should ignore it.
   *
   * add operators
   * jointAddLink:    -----q-| (q is an incomplete Joint link)
   * jointDeleteLink: -------r-| (the visual deletion of the incomplete link)
   *
   * Expected:
   * The graph doesn't contain the link
   */
  it(
    "should ignore JointJS link delete event of an incomplete link",
    marbles(m => {
      // add operators
      texeraGraph.addOperator(mockScanPredicate);
      texeraGraph.addOperator(mockResultPredicate);

      // prepare add link (incomplete link)
      const addLinkMarbleString = "-----q-|";
      const addLinkMarbleValues = {
        q: getIncompleteJointLink(mockScanResultLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellAddStream").mockReturnValue(
        m.hot(addLinkMarbleString, addLinkMarbleValues)
      );

      // prepare delete link (incomplete link)
      const deleteLinkMarbleString = "-------r-|";
      const deleteLinkMarbleValues = {
        r: getIncompleteJointLink(mockScanResultLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellDeleteStream").mockReturnValue(
        m.hot(deleteLinkMarbleString, deleteLinkMarbleValues)
      );

      // construct the texera sync model with spied dependencies
      const syncTexeraModel = new SyncTexeraModel(texeraGraph, jointGraphWrapper);

      jointGraphWrapper.getJointLinkCellAddStream().subscribe({
        complete: () => {
          expect(texeraGraph.getAllLinks().length).toEqual(0);
        },
      });
    })
  );

  /**
   * Test JointJS link change `getJointLinkCellChangeStream` event stream handled properly,
   *  when the link change involves logical link delete
   *
   * Add two operators
   * Then add a link of these operators
   * Then the user drags the target port of the connected link,
   *   the link is detached from the target port.
   * This link is now considered invalid and should be deleted from the graph
   *
   * add operators and links: 1 -> 2
   * changeLink:  -------q-| (link changes: detached from the target)
   *
   * The detatched link should be deleted from the graph.
   */
  it(
    "should delete the link when a link is detached from the target port",
    marbles(m => {
      // add operators
      texeraGraph.addOperator(mockScanPredicate);
      texeraGraph.addOperator(mockResultPredicate);

      // add links
      texeraGraph.addLink(mockScanResultLink);

      // prepare change link (link detached from target port)
      const changeLinkMarbleString = "-------q-|";
      const changeLinkMarbleValues = {
        q: getIncompleteJointLink(mockScanResultLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellChangeStream").mockReturnValue(
        m.hot(changeLinkMarbleString, changeLinkMarbleValues)
      );

      // construct the texera sync model with spied dependencies
      const syncTexeraModel = new SyncTexeraModel(texeraGraph, jointGraphWrapper);

      jointGraphWrapper.getJointLinkCellChangeStream().subscribe({
        complete: () => {
          expect(texeraGraph.getAllLinks().length).toEqual(0);
        },
      });
    })
  );

  /**
   * Test JointJS link change `getJointLinkCellChangeStream` event stream handled properly,
   *  when the link change involves logical link delete,
   *  and the same change event involves an *immediate* link add.
   *
   * Add three operators
   * Then add a link from operator 1 to operator 2
   * Then the user directly drags the target port from operator 2's input operator
   *  to operator 3's input port. The link automatically attach to operator3's target port,
   *  and JointJS only emits one link change event,
   *
   * addOperators: 1 -> 2 (will change to 1 -> 3 in after changeLink event)
   * addLink:     -------p-|
   * changeLink:  ---------t-| (link changes: target operator/port changed)
   *
   * Expected:
   * the link should be changed to the new target
   *
   */
  it(
    "should delete and then re-add the link if link target is changed from one port to another",
    marbles(m => {
      // add operators
      texeraGraph.addOperator(mockScanPredicate);
      texeraGraph.addOperator(mockSentimentPredicate);
      texeraGraph.addOperator(mockResultPredicate);

      // prepare add link
      const addLinkMarbleString = "-------p-|";
      const addLinkMarbleValues = {
        p: getJointLinkValue(mockScanResultLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellAddStream").mockReturnValue(
        m.hot(addLinkMarbleString, addLinkMarbleValues)
      );

      // create a mock changed link using another link's source/target
      // but the link ID remains the same
      const mockChangedLink = {
        ...mockScanSentimentLink,
        linkID: mockScanResultLink.linkID,
      };

      // prepare change link (link detached from target port)
      const changeLinkMarbleString = "---------t-|";
      const changeLinkMarbleValues = {
        t: getJointLinkValue(mockChangedLink),
      };
      vi.spyOn(jointGraphWrapper, "getJointLinkCellChangeStream").mockReturnValue(
        m.hot(changeLinkMarbleString, changeLinkMarbleValues)
      );

      // construct the texera sync model with spied dependencies
      const syncTexeraModel = new SyncTexeraModel(texeraGraph, jointGraphWrapper);

      jointGraphWrapper.getJointLinkCellChangeStream().subscribe({
        complete: () => {
          expect(texeraGraph.getAllLinks().length).toEqual(1);
          expect(texeraGraph.hasLinkWithID(mockChangedLink.linkID)).toBeTruthy();
          expect(texeraGraph.getLinkWithID(mockChangedLink.linkID)).toEqual(mockChangedLink);
          expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeFalsy();
          expect(texeraGraph.hasLink(mockChangedLink.source, mockChangedLink.target)).toBeTruthy();
        },
      });
    })
  );

  /**
   * `SyncTexeraModel.getOperatorLink` is the static converter the link-event
   * handlers route every valid joint link through. Its two `throw` branches
   * (missing source / target) are unreachable from the stream-handler tests
   * because `isValidJointLink` filters those cases out beforehand, so they
   * need direct invocation to be covered.
   */
  describe("getOperatorLink", () => {
    it("transforms a fully connected joint link into the matching OperatorLink", () => {
      const jointLink = getJointLinkValue(mockScanResultLink);

      expect(SyncTexeraModel.getOperatorLink(jointLink)).toEqual({
        linkID: mockScanResultLink.linkID,
        source: {
          operatorID: mockScanResultLink.source.operatorID,
          portID: mockScanResultLink.source.portID,
        },
        target: {
          operatorID: mockScanResultLink.target.operatorID,
          portID: mockScanResultLink.target.portID,
        },
      });
    });

    it("throws when the joint link has no source attribute", () => {
      const linkWithNoSource = {
        id: "no-source-link",
        attributes: {
          source: null,
          target: { id: "op-target", port: "input-0" },
        },
      } as unknown as joint.dia.Link;

      expect(() => SyncTexeraModel.getOperatorLink(linkWithNoSource)).toThrow(
        "Invalid JointJS Link: no source element"
      );
    });

    it("throws when the joint link has no target attribute", () => {
      const linkWithNoTarget = {
        id: "no-target-link",
        attributes: {
          source: { id: "op-source", port: "output-0" },
          target: null,
        },
      } as unknown as joint.dia.Link;

      expect(() => SyncTexeraModel.getOperatorLink(linkWithNoTarget)).toThrow(
        "Invalid JointJS Link: no target element"
      );
    });
  });
});

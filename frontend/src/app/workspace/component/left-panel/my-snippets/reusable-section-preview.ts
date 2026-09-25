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

import { WorkflowFragment } from "../../../types/workflow-fragment.interface";

export interface PreviewNode {
  id: string;
  label: string;
  x: number;
  y: number;
}

export interface PreviewLink {
  id: string;
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

const NODE_WIDTH = 100;
const NODE_HEIGHT = 42;
const NODE_GAP = 12;
const VIEWBOX_PADDING = 24;
const POSITION_SCALE = 0.5;
const MAX_POSITION_SCALE = 1;

/** Pure thumbnail layout; never mutates saved workflow positions. */
export function buildSectionPreview(fragment: WorkflowFragment): {
  nodes: PreviewNode[];
  links: PreviewLink[];
  viewBox: string;
} {
  const rawNodes = fragment.operators.map(operator => {
    const position = fragment.operatorPositions[operator.operatorID];
    return {
      id: operator.operatorID,
      label: operator.operatorType.split(".").pop() ?? operator.operatorType,
      x: position.x,
      y: position.y,
    };
  });
  const nodes = compactNodes(rawNodes);
  const viewBox = getContentViewBox(nodes);
  const nodeByID = new Map(nodes.map(node => [node.id, node]));
  const links = fragment.links.flatMap(link => {
    const source = nodeByID.get(link.source.operatorID);
    const target = nodeByID.get(link.target.operatorID);
    if (!source || !target) return [];
    return [
      {
        id: link.linkID,
        x1: source.x + NODE_WIDTH,
        y1: source.y + NODE_HEIGHT / 2,
        x2: target.x,
        y2: target.y + NODE_HEIGHT / 2,
      },
    ];
  });
  return { nodes, links, viewBox };
}
function compactNodes(nodes: readonly PreviewNode[]): PreviewNode[] {
  let minimumScale = POSITION_SCALE;
  for (let index = 0; index < nodes.length; index++) {
    const node = nodes[index];
    for (let otherIndex = index + 1; otherIndex < nodes.length; otherIndex++) {
      const other = nodes[otherIndex];
      const horizontalScale =
        node.x === other.x ? Number.POSITIVE_INFINITY : (NODE_WIDTH + NODE_GAP) / Math.abs(node.x - other.x);
      const verticalScale =
        node.y === other.y ? Number.POSITIVE_INFINITY : (NODE_HEIGHT + NODE_GAP) / Math.abs(node.y - other.y);
      const requiredScale = Math.min(horizontalScale, verticalScale);
      if (Number.isFinite(requiredScale)) minimumScale = Math.max(minimumScale, requiredScale);
    }
  }

  const positionScale = Math.min(minimumScale, MAX_POSITION_SCALE);
  const compacted = nodes.map(node => ({
    ...node,
    x: node.x * positionScale,
    y: node.y * positionScale,
  }));

  // Source layouts can contain overlapping operators. Uniform scaling cannot
  // resolve those, so move each remaining collision past an earlier node.
  compacted.forEach((node, index) => {
    const earlierNodes = compacted.slice(0, index);
    for (let pass = 0; pass <= earlierNodes.length; pass++) {
      const other = earlierNodes.find(candidate => {
        const overlapX = NODE_WIDTH + NODE_GAP - Math.abs(node.x - candidate.x);
        const overlapY = NODE_HEIGHT + NODE_GAP - Math.abs(node.y - candidate.y);
        return overlapX > 0 && overlapY > 0;
      });
      if (!other) break;
      const overlapX = NODE_WIDTH + NODE_GAP - Math.abs(node.x - other.x);
      const overlapY = NODE_HEIGHT + NODE_GAP - Math.abs(node.y - other.y);
      if (overlapX <= overlapY) {
        node.x = other.x + NODE_WIDTH + NODE_GAP;
      } else {
        node.y = other.y + NODE_HEIGHT + NODE_GAP;
      }
    }
  });
  return compacted;
}

function getContentViewBox(nodes: readonly PreviewNode[]): string {
  const minX = Math.min(...nodes.map(node => node.x));
  const minY = Math.min(...nodes.map(node => node.y));
  const maxX = Math.max(...nodes.map(node => node.x + NODE_WIDTH));
  const maxY = Math.max(...nodes.map(node => node.y + NODE_HEIGHT));
  const padding = VIEWBOX_PADDING;
  return `${minX - padding} ${minY - padding} ${maxX - minX + padding * 2} ${maxY - minY + padding * 2}`;
}

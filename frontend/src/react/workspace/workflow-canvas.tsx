/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useEffect, useRef, useSyncExternalStore } from "react";
import * as joint from "jointjs";
import concaveman from "concaveman";
import { line, curveCatmullRomClosed } from "d3-shape";
import { of } from "rxjs";
import { JointUIService as JointUI } from "../../core/workspace/service/joint-ui/joint-ui";
import { JointGraphWrapper } from "../../core/workspace/service/workflow-graph/model/joint-graph-wrapper";
import type { CoeditorState } from "../../core/common/type/user";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
import { WorkflowSession } from "./workflow-session";
import { CanvasModel } from "./canvas-model";
import type { RuntimeSnapshot } from "./workflow-runtime";
import type { HeatmapView } from "../../core/workspace/service/heatmap/heatmap-scoring";
import { paintCanvasStatus, paintVersionHighlights } from "./canvas-status";
import type { VersionHighlights } from "../workflows/version-diff";
import { canConnect } from "./connection-validation";
import type { Point } from "../../core/workspace/types/workflow-common.interface";
import "jointjs/css/layout.css";
import "jointjs/css/themes/default.css";
import "./workspace.css";

export interface CanvasActions {
  zoom(delta: number): void;
  reset(): void;
  fit(): void;
  autoLayout(): void;
  capturePng(): Promise<Blob>;
}
export interface WorkflowCanvasProps {
  model: CanvasModel;
  session: WorkflowSession;
  metadata: OperatorMetadata;
  selected: readonly string[];
  runtimeSnapshot?: RuntimeSnapshot;
  invalidOperatorIds?: readonly string[];
  versionHighlights?: VersionHighlights;
  heatmap?: HeatmapView | null;
  regions?: boolean;
  onDropOperator?: (type: string, position: Point) => void;
  onHover?: (id: string | undefined, anchor?: Point) => void;
  onSelect(id: string | undefined, append: boolean): void;
  onEdit?(id: string): void;
  onReady?(actions: CanvasActions | undefined): void;
  onError(error: Error): void;
  onContextMenu?(id: string, x: number, y: number): void;
}

export function WorkflowCanvas(props: WorkflowCanvasProps) {
  const host = useRef<HTMLDivElement>(null);
  const minimapHost = useRef<HTMLDivElement>(null);
  const boxHost = useRef<HTMLDivElement>(null);
  const workflowState = useSyncExternalStore(props.session.subscribe, props.session.getSnapshot);
  const latest = useRef(props);
  latest.current = props;
  const activePaper = useRef<joint.dia.Paper | undefined>(undefined);
  useEffect(() => {
    const { model, session, metadata } = latest.current;
    const shared = session.shared;
    const node = host.current!;
    const wrapper = new JointGraphWrapper(model.graph);
    const ui = new JointUI({ getOperatorMetadata: () => of(metadata) });
    const paper = wrapper.attachMainJointPaper({
      el: node,
      width: node.clientWidth || 800,
      height: node.clientHeight || 600,
      gridSize: 10,
      drawGrid: true,
      background: { color: "#fff" },
      cellViewNamespace: joint.shapes,
      interactive: view =>
        session.getSnapshot().canEdit && !view.model.get("texeraOverlay") && !view.model.get("texeraPresence")
          ? true
          : {
              elementMove: false,
              linkMove: false,
              vertexMove: false,
              arrowheadMove: false,
              vertexAdd: false,
              vertexRemove: false,
              labelMove: false,
            },
      defaultLink: () => JointUI.getDefaultLinkCell(),
      linkPinning: false,
      snapLinks: { radius: 40 },
      markAvailable: true,
      validateMagnet: (_view, magnet) => session.getSnapshot().canEdit && magnet.getAttribute("port-group") === "out",
      validateConnection: (sourceView, sourceMagnet, targetView, targetMagnet) => {
        if (!session.getSnapshot().canEdit || !sourceMagnet || !targetMagnet) return false;
        const sourcePort = sourceMagnet.getAttribute("port") ?? sourceMagnet.closest("[port]")?.getAttribute("port");
        const targetPort = targetMagnet.getAttribute("port") ?? targetMagnet.closest("[port]")?.getAttribute("port");
        if (!sourcePort || !targetPort) return false;
        return canConnect(
          session.graph,
          metadata,
          String(sourceView.model.id),
          sourcePort,
          String(targetView.model.id),
          targetPort
        );
      },
    });
    activePaper.current = paper;
    const minimap = new joint.dia.Paper({
      el: minimapHost.current!,
      model: model.graph,
      width: 180,
      height: 120,
      interactive: false,
      cellViewNamespace: joint.shapes,
    });
    const fitMap = () => minimap.scaleContentToFit({ padding: 8, minScale: 0.05, maxScale: 0.3 });
    const jump = (_event: unknown, x: number, y: number) => {
      const scale = paper.scale();
      paper.translate(node.clientWidth / 2 - x * scale.sx, node.clientHeight / 2 - y * scale.sy);
    };
    minimap.on("blank:pointerdown", jump);
    minimap.on("element:pointerdown", (_view, _event, x, y) => jump(_event, x, y));
    model.graph.on("add remove change:position", fitMap);
    fitMap();
    let drag: { id: string; positions: Map<string, Point> } | undefined,
      moving = false;
    paper.on("element:pointerdown", view => {
      const id = String(view.model.id);
      if (!session.getSnapshot().canEdit || (!session.graph.hasOperator(id) && !session.graph.hasCommentBox(id)))
        return;
      session.beginGesture();
      const ids = latest.current.selected.includes(id) ? latest.current.selected : [id];
      drag = {
        id,
        positions: new Map(
          ids.flatMap(id => {
            const cell = model.graph.getCell(id);
            return cell?.isElement() ? [[id, (cell as joint.dia.Element).position()] as const] : [];
          })
        ),
      };
    });
    const moveSelection = (cell: joint.dia.Element) => {
      if (moving || !drag || String(cell.id) !== drag.id) return;
      const original = drag.positions.get(drag.id);
      if (!original) return;
      const offset = { x: cell.position().x - original.x, y: cell.position().y - original.y };
      moving = true;
      try {
        for (const [id, point] of drag.positions)
          if (id !== drag.id)
            (model.graph.getCell(id) as joint.dia.Element | undefined)?.position(
              point.x + offset.x,
              point.y + offset.y
            );
      } finally {
        moving = false;
      }
    };
    model.graph.on("change:position", moveSelection);
    paper.on("element:pointerclick", (view, event) =>
      latest.current.onSelect(String(view.model.id), Boolean(event.ctrlKey || event.metaKey || event.shiftKey))
    );
    paper.on("link:pointerclick", (view, event) =>
      latest.current.onSelect(String(view.model.id), Boolean(event.ctrlKey || event.metaKey || event.shiftKey))
    );
    paper.on("blank:pointerclick", () => latest.current.onSelect(undefined, false));
    paper.on("element:pointerdblclick", (view: joint.dia.ElementView) =>
      latest.current.onEdit?.(String(view.model.id))
    );
    paper.on("element:contextmenu", (view, event) => {
      event.preventDefault();
      latest.current.onContextMenu?.(String(view.model.id), event.clientX ?? 0, event.clientY ?? 0);
    });
    paper.on("element:mouseenter", (view, event) => {
      if (session.graph.hasOperator(String(view.model.id))) {
        ui.unfoldOperatorDetails(paper, String(view.model.id), session.getSnapshot().canEdit);
        if (!event.buttons) {
          const box = view.el.getBoundingClientRect();
          latest.current.onHover?.(String(view.model.id), { x: box.right + 12, y: box.top });
        }
      }
    });
    paper.on("element:mouseleave", (view: joint.dia.ElementView) => {
      if (session.graph.hasOperator(String(view.model.id))) ui.foldOperatorDetails(paper, String(view.model.id));
      latest.current.onHover?.(undefined);
    });
    paper.on("element:delete", (view: joint.dia.ElementView, event: MouseEvent) => {
      event.stopPropagation();
      try {
        session.deleteOperators([String(view.model.id)]);
      } catch (error) {
        latest.current.onError(error as Error);
      }
    });
    let pan: { x: number; y: number; tx: number; ty: number } | undefined;
    let box: { x: number; y: number; clientX: number; clientY: number } | undefined;
    paper.on("blank:pointerdown", (event, x: number, y: number) => {
      if (event.shiftKey) {
        box = { x, y, clientX: event.clientX ?? 0, clientY: event.clientY ?? 0 };
        return;
      }
      const translation = paper.translate();
      pan = { x, y, tx: translation.tx, ty: translation.ty };
    });
    const move = (event: PointerEvent) => {
      const point = paper.clientToLocalPoint(event.clientX, event.clientY);
      shared.updateAwareness("userCursor", { x: point.x, y: point.y });
      if (box && boxHost.current) {
        const bounds = node.getBoundingClientRect();
        Object.assign(boxHost.current.style, {
          display: "block",
          left: `${Math.min(event.clientX, box.clientX) - bounds.left}px`,
          top: `${Math.min(event.clientY, box.clientY) - bounds.top}px`,
          width: `${Math.abs(event.clientX - box.clientX)}px`,
          height: `${Math.abs(event.clientY - box.clientY)}px`,
        });
      }
      if (pan && event.buttons)
        paper.translate(pan.tx + (point.x - pan.x) * paper.scale().sx, pan.ty + (point.y - pan.y) * paper.scale().sy);
    };
    const stopPan = (event: PointerEvent) => {
      if (box) {
        const point = paper.clientToLocalPoint(event.clientX, event.clientY),
          rect = new joint.g.Rect(
            Math.min(box.x, point.x),
            Math.min(box.y, point.y),
            Math.abs(box.x - point.x),
            Math.abs(box.y - point.y)
          );
        const ids = paper
          .findViewsInArea(rect)
          .map(view => String(view.model.id))
          .filter(id => session.graph.hasOperator(id) || session.graph.hasCommentBox(id));
        latest.current.onSelect(undefined, false);
        ids.forEach(id => latest.current.onSelect(id, true));
        session.graph
          .getAllLinks()
          .filter(link => ids.includes(link.source.operatorID) && ids.includes(link.target.operatorID))
          .forEach(link => latest.current.onSelect(link.linkID, true));
      }
      if (boxHost.current) boxHost.current.style.display = "none";
      pan = undefined;
      box = undefined;
      drag = undefined;
      session.endGesture();
    };
    const enter = () => shared.updateAwareness("isActive", true);
    const leave = () => {
      shared.updateAwareness("isActive", false);
      pan = undefined;
    };
    node.addEventListener("pointermove", move);
    node.addEventListener("pointerenter", enter);
    node.addEventListener("pointerleave", leave);
    window.addEventListener("pointerup", stopPan);
    const dragover = (event: DragEvent) => {
      if (session.getSnapshot().canEdit && event.dataTransfer?.types.includes("application/x-texera-operator")) {
        event.preventDefault();
        event.dataTransfer.dropEffect = "copy";
      }
    };
    const drop = (event: DragEvent) => {
      const type = event.dataTransfer?.getData("application/x-texera-operator");
      if (type && session.getSnapshot().canEdit) {
        event.preventDefault();
        latest.current.onDropOperator?.(type, paper.clientToLocalPoint(event.clientX, event.clientY));
      }
    };
    node.addEventListener("dragover", dragover);
    node.addEventListener("drop", drop);
    const wheel = (event: WheelEvent) => {
      if (!event.ctrlKey && !event.metaKey) return;
      event.preventDefault();
      const ratio = Math.min(1.3, Math.max(0.7, paper.scale().sx + (event.deltaY < 0 ? 0.05 : -0.05)));
      paper.scale(ratio, ratio);
      wrapper.setZoomProperty(ratio);
    };
    node.addEventListener("wheel", wheel, { passive: false });
    const resize =
      typeof ResizeObserver === "undefined"
        ? undefined
        : new ResizeObserver(() => paper.setDimensions(node.clientWidth, node.clientHeight));
    resize?.observe(node);
    const presence = () => {
      const visible = new Set<string>();
      shared.awareness.getStates().forEach((raw, clientId) => {
        const state = raw as CoeditorState;
        if (clientId === shared.awareness.clientID || !state?.user || !state.isActive || !state.userCursor) return;
        const id = JointUI.getJointUserPointerName(state.user);
        visible.add(id);
        const current = model.graph.getCell(id) as joint.dia.Element | undefined;
        if (current) current.position(state.userCursor.x, state.userCursor.y);
        else {
          const pointer = JointUI.getJointUserPointerCell(state.user, state.userCursor, state.user.color ?? "#1677ff");
          pointer.set("texeraPresence", true);
          model.graph.addCell(pointer);
        }
      });
      model.graph
        .getCells()
        .filter(cell => cell.get("texeraPresence") && !visible.has(String(cell.id)))
        .forEach(cell => cell.remove());
    };
    shared.awareness.on("change", presence);
    presence();
    latest.current.onReady?.({
      zoom: delta => {
        const ratio = Math.min(1.3, Math.max(0.7, paper.scale().sx + delta));
        wrapper.setZoomProperty(ratio);
        paper.scale(ratio, ratio);
      },
      reset: () => {
        wrapper.restoreDefaultZoomAndOffset();
        paper.scale(1, 1);
        paper.translate(0, 0);
      },
      fit: () => paper.scaleContentToFit({ padding: 50, minScale: 0.7, maxScale: 1.3 }),
      autoLayout: () => wrapper.autoLayoutJoint(),
      capturePng: async () => {
        const { default: html2canvas } = await import("html2canvas");
        const canvas = await html2canvas(node, { backgroundColor: "#fff" });
        return await new Promise<Blob>((resolve, reject) =>
          canvas.toBlob(
            blob => (blob ? resolve(blob) : reject(new Error("The canvas image could not be exported."))),
            "image/png"
          )
        );
      },
    });
    return () => {
      latest.current.onReady?.(undefined);
      activePaper.current = undefined;
      resize?.disconnect();
      session.endGesture();
      minimap.remove();
      model.graph.off("add remove change:position", fitMap);
      model.graph.off("change:position", moveSelection);
      node.removeEventListener("dragover", dragover);
      node.removeEventListener("drop", drop);
      node.removeEventListener("pointermove", move);
      node.removeEventListener("pointerenter", enter);
      node.removeEventListener("pointerleave", leave);
      node.removeEventListener("wheel", wheel);
      window.removeEventListener("pointerup", stopPan);
      shared.awareness.off("change", presence);
      shared.updateAwareness("isActive", false);
      wrapper.detachMainJointPaper(paper);
      paper.remove();
    };
  }, [props.model, props.session]);
  useEffect(() => {
    const paper = activePaper.current;
    if (!paper) return;
    props.model.graph
      .getElements()
      .forEach(cell =>
        paper.findViewByModel(cell)?.el.classList.toggle("is-selected", props.selected.includes(String(cell.id)))
      );
  }, [props.model, props.selected]);
  useEffect(() => {
    const paper = activePaper.current;
    if (!paper) return;
    const ui = new JointUI({ getOperatorMetadata: () => of(props.metadata) });
    paintCanvasStatus(
      ui,
      paper,
      workflowState.workflow.content.operators,
      props.runtimeSnapshot,
      props.invalidOperatorIds ?? [],
      props.heatmap ?? null
    );
    paintVersionHighlights(props.model.graph, props.versionHighlights);
    const ids = new Set<string>();
    for (const [id, operators] of props.runtimeSnapshot?.regions ?? []) {
      const cells = operators.flatMap(id => {
        const cell = props.model.graph.getCell(id);
        return cell ? [cell] : [];
      });
      if (!cells.length) continue;
      const regionId = `region-${id}`;
      ids.add(regionId);
      let cell = props.model.graph.getCell(regionId) as joint.dia.Element | undefined;
      if (!cell) {
        cell = new joint.dia.Element({
          id: regionId,
          type: "texera.Region",
          markup: [{ tagName: "path", selector: "body" }],
          texeraOverlay: true,
          z: -1,
        });
        props.model.graph.addCell(cell);
      }
      const points = cells.flatMap(cell => {
        const { x, y, width, height } = cell.getBBox();
        return [
          [x - 15, y - 15],
          [x + width + 15, y - 15],
          [x - 15, y + height + 25],
          [x + width + 15, y + height + 25],
        ];
      });
      const color: Record<string, string> = {
        ExecutingDependeePortsPhase: "rgba(33,150,243,0.2)",
        ExecutingNonDependeePortsPhase: "rgba(255,213,79,0.2)",
        Completed: "rgba(76,175,80,0.2)",
      };
      cell.attr({
        body: {
          d: line().curve(curveCatmullRomClosed)(concaveman(points, 2, 0) as [number, number][]),
          fill: color[props.runtimeSnapshot?.regionStates[id] ?? ""] ?? "rgba(158,158,158,0.2)",
          pointerEvents: "none",
          visibility: props.regions ? "visible" : "hidden",
        },
      });
    }
    props.model.graph
      .getCells()
      .filter(cell => cell.get("texeraOverlay") && !ids.has(String(cell.id)))
      .forEach(cell => cell.remove());
  }, [
    props.metadata,
    props.model,
    props.runtimeSnapshot,
    props.invalidOperatorIds,
    props.versionHighlights,
    props.heatmap,
    props.regions,
    workflowState,
  ]);
  return (
    <div className="react-canvas-container">
      <div
        ref={host}
        className="react-workflow-canvas"
        role="application"
        aria-label="Workflow canvas"
        tabIndex={0}
      />
      <div
        ref={minimapHost}
        className="react-canvas-minimap"
        aria-label="Workflow minimap"
      />
      <div
        ref={boxHost}
        className="react-canvas-selection-box"
      />
    </div>
  );
}

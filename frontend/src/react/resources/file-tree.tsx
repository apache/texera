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
import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import type { StorageFileNode } from "./storage-api";
import { formatQuotaSize } from "./quota";

export const FILE_TREE_ROW_HEIGHT = 32;
const MAX_HEIGHT = 480;
const OVERSCAN = 6;
const path = (node: StorageFileNode) => `${node.parentDir}/${node.name}`;
export function ResourceFileTree({
  nodes,
  onOpen,
  selectedPath,
}: {
  nodes: StorageFileNode[];
  onOpen: (node: StorageFileNode) => void;
  selectedPath?: string;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());
  const [scrollTop, setScrollTop] = useState(0);
  const [focused, setFocused] = useState<number>();
  const rows = useMemo(() => {
    const result: { node: StorageFileNode; depth: number; key: string }[] = [];
    const stack = nodes.map(node => ({ node, depth: 0 })).reverse();
    while (stack.length) {
      const entry = stack.pop()!;
      const key = path(entry.node);
      result.push({ ...entry, key });
      if (entry.node.type === "directory" && !collapsed.has(key))
        for (const child of [...(entry.node.children ?? [])].reverse())
          stack.push({ node: child, depth: entry.depth + 1 });
    }
    return result;
  }, [nodes, collapsed]);
  const height = Math.max(FILE_TREE_ROW_HEIGHT, Math.min(MAX_HEIGHT, rows.length * FILE_TREE_ROW_HEIGHT));
  const first = Math.max(
    0,
    Math.min(
      Math.floor(scrollTop / FILE_TREE_ROW_HEIGHT) - OVERSCAN,
      Math.max(0, rows.length - Math.ceil(height / FILE_TREE_ROW_HEIGHT) - OVERSCAN)
    )
  );
  const last = Math.min(rows.length, first + Math.ceil(height / FILE_TREE_ROW_HEIGHT) + OVERSCAN * 2);
  useEffect(() => {
    const element = viewport.current;
    if (!element) return;
    const max = Math.max(0, rows.length * FILE_TREE_ROW_HEIGHT - height);
    if (element.scrollTop > max) {
      element.scrollTop = max;
      setScrollTop(max);
    }
  }, [rows.length, height]);
  useEffect(() => {
    if (focused === undefined) return;
    viewport.current?.querySelector<HTMLButtonElement>(`[data-tree-index="${focused}"] button`)?.focus();
  }, [focused, first, last]);
  const toggle = (key: string) =>
    setCollapsed(previous => {
      const next = new Set(previous);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  function key(event: KeyboardEvent, index: number) {
    const row = rows[index];
    if (event.key === "ArrowRight" && row.node.type === "directory") {
      event.preventDefault();
      setCollapsed(previous => {
        const next = new Set(previous);
        next.delete(row.key);
        return next;
      });
      return;
    }
    if (event.key === "ArrowLeft" && row.node.type === "directory") {
      event.preventDefault();
      setCollapsed(previous => new Set(previous).add(row.key));
      return;
    }
    const target =
      event.key === "ArrowDown"
        ? Math.min(rows.length - 1, index + 1)
        : event.key === "ArrowUp"
          ? Math.max(0, index - 1)
          : event.key === "Home"
            ? 0
            : event.key === "End"
              ? rows.length - 1
              : undefined;
    if (target === undefined) return;
    event.preventDefault();
    const element = viewport.current!;
    if (target * FILE_TREE_ROW_HEIGHT < element.scrollTop) element.scrollTop = target * FILE_TREE_ROW_HEIGHT;
    else if ((target + 1) * FILE_TREE_ROW_HEIGHT > element.scrollTop + height)
      element.scrollTop = (target + 1) * FILE_TREE_ROW_HEIGHT - height;
    setScrollTop(element.scrollTop);
    setFocused(target);
  }
  return (
    <div
      ref={viewport}
      role="tree"
      aria-label="Resource files"
      style={{ height, overflow: "auto", position: "relative", border: "1px solid #e5e7eb" }}
      onScroll={event => setScrollTop(event.currentTarget.scrollTop)}>
      {!rows.length ? (
        <p>No files in this version.</p>
      ) : (
        <div style={{ height: rows.length * FILE_TREE_ROW_HEIGHT, position: "relative" }}>
          {rows.slice(first, last).map((row, offset) => {
            const index = first + offset;
            const directory = row.node.type === "directory";
            return (
              <div
                key={row.key}
                role="treeitem"
                aria-level={row.depth + 1}
                aria-expanded={directory ? !collapsed.has(row.key) : undefined}
                aria-selected={selectedPath === row.key}
                data-tree-index={index}
                style={{
                  position: "absolute",
                  top: index * FILE_TREE_ROW_HEIGHT,
                  height: FILE_TREE_ROW_HEIGHT,
                  left: 0,
                  right: 0,
                  paddingLeft: row.depth * 16,
                  background: selectedPath === row.key ? "#e6f4ff" : undefined,
                }}>
                <button
                  type="button"
                  aria-label={
                    directory
                      ? `${collapsed.has(row.key) ? "Expand" : "Collapse"} directory ${row.node.name}`
                      : `Open file ${row.node.name}`
                  }
                  style={{
                    height: "100%",
                    maxWidth: "100%",
                    border: 0,
                    background: "transparent",
                    textAlign: "left",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    padding: "0 8px",
                  }}
                  onKeyDown={event => key(event, index)}
                  onClick={() => (directory ? toggle(row.key) : onOpen(row.node))}>
                  {directory ? (collapsed.has(row.key) ? "▸ " : "▾ ") : ""}
                  {row.node.name}
                  {row.node.size === undefined ? "" : ` (${formatQuotaSize(row.node.size)})`}
                </button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

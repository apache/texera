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

import { useCallback, useId, useSyncExternalStore } from "react";
import type * as Y from "yjs";

export interface CollaborativeTextProps {
  text: Y.Text;
  label: string;
  disabled?: boolean;
  multiline?: boolean;
  onChange?: (value: string) => void;
  placeholder?: string;
  invalid?: boolean;
  describedBy?: string;
}

/**
 * Native text controls bound to Y.Text. Only the changed span is replaced, preserving
 * shared anchors and concurrent edits outside it. Remote transactions never echo as local edits.
 */
export function CollaborativeText({
  text,
  label,
  disabled,
  multiline,
  onChange,
  placeholder,
  invalid,
  describedBy,
}: CollaborativeTextProps) {
  const id = useId();
  const subscribe = useCallback(
    (changed: () => void) => {
      text.observe(changed);
      return () => text.unobserve(changed);
    },
    [text]
  );
  const snapshot = useCallback(() => text.toString(), [text]);
  const value = useSyncExternalStore(subscribe, snapshot, snapshot);
  const update = (next: string) => {
    if (disabled || next === text.toString()) return;
    const current = text.toString();
    let start = 0;
    while (start < current.length && start < next.length && current[start] === next[start]) start++;
    // Yjs offsets, like DOM selection offsets, use UTF-16. Keep surrogate pairs together.
    if (start > 0 && start < current.length && /[\uDC00-\uDFFF]/.test(current[start])) start--;
    let oldEnd = current.length;
    let newEnd = next.length;
    while (oldEnd > start && newEnd > start && current[oldEnd - 1] === next[newEnd - 1]) {
      oldEnd--;
      newEnd--;
    }
    if (oldEnd < current.length && /[\uDC00-\uDFFF]/.test(current[oldEnd])) {
      oldEnd++;
      newEnd++;
    }
    const edit = () => {
      if (oldEnd > start) text.delete(start, oldEnd - start);
      if (newEnd > start) text.insert(start, next.slice(start, newEnd));
    };
    if (text.doc) text.doc.transact(edit, text);
    else edit();
    onChange?.(next);
  };
  const shared = {
    id,
    value,
    disabled,
    placeholder,
    "aria-invalid": invalid || undefined,
    "aria-describedby": describedBy,
    onChange: (event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => update(event.currentTarget.value),
  };
  return (
    <div className="property-collaborative-text">
      <label htmlFor={id}>{label}</label>
      {multiline ? (
        <textarea
          {...shared}
          rows={5}
        />
      ) : (
        <input
          {...shared}
          type="text"
        />
      )}
    </div>
  );
}

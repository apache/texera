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

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { CollaborativeText } from "./collaborative-text";

afterEach(cleanup);

describe("CollaborativeText", () => {
  it("writes local text to Y.Text and displays remote edits without echoing them as local input", () => {
    const doc = new Y.Doc();
    const text = doc.getText("name");
    text.insert(0, "old");
    const onChange = vi.fn();
    render(<CollaborativeText text={text} label="Name" onChange={onChange} />);
    fireEvent.change(screen.getByRole("textbox", { name: "Name" }), { target: { value: "東京 🐧" } });
    expect(text.toString()).toBe("東京 🐧");
    expect(onChange).toHaveBeenCalledExactlyOnceWith("東京 🐧");
    act(() => { doc.transact(() => text.insert(text.length, "!"), "remote"); });
    expect((screen.getByRole("textbox", { name: "Name" }) as HTMLInputElement).value).toBe("東京 🐧!");
    expect(onChange).toHaveBeenCalledTimes(1);
    doc.destroy();
  });

  it("detaches the previous text on property changes and all observers on unmount", () => {
    const doc = new Y.Doc();
    const first = doc.getText("first");
    const second = doc.getText("second");
    first.insert(0, "first"); second.insert(0, "second");
    const unobserveFirst = vi.spyOn(first, "unobserve");
    const unobserveSecond = vi.spyOn(second, "unobserve");
    const { rerender, unmount } = render(<CollaborativeText text={first} label="Name" />);
    rerender(<CollaborativeText text={second} label="Name" />);
    expect(unobserveFirst).toHaveBeenCalledTimes(1);
    act(() => first.insert(0, "obsolete"));
    expect((screen.getByRole("textbox", { name: "Name" }) as HTMLInputElement).value).toBe("second");
    unmount();
    expect(unobserveSecond).toHaveBeenCalledTimes(1);
    doc.destroy();
  });

  it("preserves disabled state and performs minimal edits that retain shared text positions", () => {
    const doc = new Y.Doc();
    const text = doc.getText("name");
    text.insert(0, "alpha beta");
    const anchor = Y.createRelativePositionFromTypeIndex(text, 6);
    const { rerender } = render(<CollaborativeText text={text} label="Name" disabled />);
    expect((screen.getByRole("textbox", { name: "Name" }) as HTMLInputElement).disabled).toBe(true);
    rerender(<CollaborativeText text={text} label="Name" />);
    fireEvent.change(screen.getByRole("textbox", { name: "Name" }), { target: { value: "α alpha beta" } });
    expect(text.toString()).toBe("α alpha beta");
    expect(Y.createAbsolutePositionFromRelativePosition(anchor, doc)?.index).toBe(8);
    fireEvent.change(screen.getByRole("textbox", { name: "Name" }), { target: { value: "" } });
    expect(text.length).toBe(0);
    doc.destroy();
  });
});

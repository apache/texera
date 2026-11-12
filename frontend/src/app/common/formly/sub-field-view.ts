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

/**
 * What the Form View's author decided about each sub-field of an exposed input, resolved for the
 * reader in front of it, for a widget that draws its own chrome.
 *
 * Formly expresses a rename as the field's label and a hide as `hide`, and renders both through the
 * field's wrapper. A widget that renders its sub-fields itself -- a table with its own column
 * headers, a legend, a summary line -- never goes through that wrapper, so it shows neither: a
 * renamed column keeps its old heading, and a hidden one keeps its heading over an empty cell
 * (#8763). Reading the state back out of the rendered fields is not the answer either: it is only
 * there once formly has built a row, so a table whose operator declares nothing yet has nothing to
 * read, and a group's hide is marked on the group while its children are merely not rendered.
 *
 * So the page that holds the author's decisions publishes them, once, on the field it hands the
 * widget ({@link SUB_FIELD_VIEW_PROP}), and the widget asks here. Nothing published -- the operator
 * property panel, where there is no author and no overrides -- means every sub-field is in view
 * under its own name, which is what a widget's fixed headers already say.
 */
export interface SubFieldView {
  /**
   * The name the author gave this sub-field, or "" where they kept the one it came with: a widget
   * has a name of its own for the part it draws (a table's column heading) and keeps it then.
   */
  readonly label: string;
  /** Whether this sub-field is in view at all. False only for a reader looking at a hidden one. */
  readonly shown: boolean;
}

/** Where a host publishes {@link SubFieldView}s, keyed by sub-field path, in the field's `props`. */
export const SUB_FIELD_VIEW_PROP = "subFieldView";

/** What the host published for this field, if it is a host that publishes. */
export function subFieldViews(field: {
  props?: Record<string, unknown>;
}): Readonly<Record<string, SubFieldView>> | undefined {
  const published = field.props?.[SUB_FIELD_VIEW_PROP];
  return typeof published === "object" && published !== null && !Array.isArray(published)
    ? (published as Readonly<Record<string, SubFieldView>>)
    : undefined;
}

/** A sub-field's name: the author's where they gave one, else the one the widget draws itself. */
export function subFieldLabel(field: { props?: Record<string, unknown> }, path: string, fallback: string): string {
  const label = subFieldViews(field)?.[path]?.label;
  return label ? label : fallback;
}

/**
 * The parts of a widget's own layout the reader should see, in the widget's order: everything, bar
 * what the author hid. A part the host said nothing about stays -- it is one the author was never
 * offered, not one they hid.
 *
 * The same array instance comes back while the answer has not changed, so a widget may call this
 * from its template (the state arrives after the first check, so it has to be read live) without
 * an `ngFor` tearing its rows down on every pass.
 */
export function subFieldsInView<T extends { readonly path: string }>(
  field: { props?: Record<string, unknown> },
  parts: readonly T[]
): readonly T[] {
  const views = subFieldViews(field);
  const next = views === undefined ? parts : parts.filter(part => shown(views, part.path));
  const previous = lastInView.get(field) as readonly T[] | undefined;
  if (previous !== undefined && previous.length === next.length && previous.every((p, i) => p === next[i])) {
    return previous;
  }
  lastInView.set(field, next);
  return next;
}

/**
 * Whether a path is in view: its own entry, and every group it sits in. Hiding a group hides what
 * is inside it, and the author hid the group, so that is the entry the host published.
 */
function shown(views: Readonly<Record<string, SubFieldView>>, path: string): boolean {
  const segments = path.split(".");
  return segments.every((_, i) => views[segments.slice(0, i + 1).join(".")]?.shown !== false);
}

/** Per field, the array {@link subFieldsInView} last returned, so an unchanged answer keeps its identity. */
const lastInView = new WeakMap<object, readonly unknown[]>();

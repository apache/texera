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

import { TestBed } from "@angular/core/testing";
import { SafeHtml } from "@angular/platform-browser";

import { HighlightSearchTermsPipe } from "./highlight-search-terms.pipe";

describe("HighlightSearchTermsPipe", () => {
  let pipe: HighlightSearchTermsPipe;

  // Recover the raw HTML string that was wrapped by bypassSecurityTrustHtml.
  const raw = (safe: SafeHtml): string => (safe as any).changingThisBreaksApplicationSecurity;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [HighlightSearchTermsPipe] });
    pipe = TestBed.inject(HighlightSearchTermsPipe);
  });

  it("returns an empty string when the value is undefined", () => {
    expect(raw(pipe.transform(undefined, ["x"]))).toBe("");
  });

  it("returns the value unchanged when there are no terms to highlight", () => {
    expect(raw(pipe.transform("hello", []))).toBe("hello");
  });

  it("wraps a single matching token in a highlight span", () => {
    expect(raw(pipe.transform("hello world", ["world"]))).toBe(
      'hello <span class="highlight-search-terms">world</span>'
    );
  });

  it("matches case-insensitively and preserves the original casing inside the span", () => {
    expect(raw(pipe.transform("Hello", ["hello"]))).toBe('<span class="highlight-search-terms">Hello</span>');
  });

  it("wraps every occurrence of a token (global flag)", () => {
    expect(raw(pipe.transform("a a a", ["a"]))).toBe(
      '<span class="highlight-search-terms">a</span> ' +
        '<span class="highlight-search-terms">a</span> ' +
        '<span class="highlight-search-terms">a</span>'
    );
  });

  it("wraps each token independently when multiple terms are given", () => {
    expect(raw(pipe.transform("hello world", ["hello", "world"]))).toBe(
      '<span class="highlight-search-terms">hello</span> ' + '<span class="highlight-search-terms">world</span>'
    );
  });
});

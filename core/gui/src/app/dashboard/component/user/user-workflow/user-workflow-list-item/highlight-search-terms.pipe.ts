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

import { Pipe, PipeTransform } from "@angular/core";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
@Pipe({
  name: "highlightSearchTerms",
})
export class HighlightSearchTermsPipe implements PipeTransform {
  constructor(private sanitizer: DomSanitizer) {}

  transform(value: string | undefined, terms: string[]): SafeHtml {
    if (!terms || !terms.length || !value) {
      // Return the original value if there's nothing to highlight or if the value is undefined
      return this.sanitizer.bypassSecurityTrustHtml(value || "");
    }

    // Escape the terms to be used in a RegExp
    const regex = new RegExp(`(${terms.join("|")})`, "gi");

    const highlightedString = value.replace(regex, "<span class=\"highlight-search-terms\">$1</span>");
    // Use the sanitizer to avoid security risks
    return this.sanitizer.bypassSecurityTrustHtml(highlightedString);
  }
}

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

import { AfterViewInit, Component, ElementRef, OnDestroy } from "@angular/core";
import { createElement } from "react";
import { createRoot, Root } from "react-dom/client";
import { AboutContent } from "./about-content";

/**
 * Hosts the public About page in the Angular router.
 */
@Component({
  selector: "texera-about",
  template: "",
})
export class AboutComponent implements AfterViewInit, OnDestroy {
  private root?: Root;

  constructor(private readonly host: ElementRef<HTMLElement>) {}

  ngAfterViewInit(): void {
    this.root = createRoot(this.host.nativeElement);
    this.root.render(createElement(AboutContent));
  }

  ngOnDestroy(): void {
    this.root?.unmount();
  }
}

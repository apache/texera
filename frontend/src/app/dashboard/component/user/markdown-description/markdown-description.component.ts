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

import {
  Component,
  Input,
  Output,
  EventEmitter,
  OnInit,
  OnChanges,
  SimpleChanges,
  ViewChild,
  ElementRef,
  inject,
} from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { MarkdownService } from "ngx-markdown";

@Component({
  selector: "texera-markdown-description",
  templateUrl: "./markdown-description.component.html",
  styleUrls: ["./markdown-description.component.scss"],
})
export class MarkdownDescriptionComponent implements OnInit, OnChanges {
  private modalData = inject(NZ_MODAL_DATA, { optional: true });

  @Input() description = "";
  @Output() descriptionChange = new EventEmitter<string>();
  @ViewChild("textarea") textareaRef!: ElementRef<HTMLTextAreaElement>;

  isEditing = false;
  editingContent = "";
  renderedDescription = "";

  readonly toolbar: { icon: string; tip: string; prefix: string; suffix: string; default: string }[] = [
    { icon: "bold", tip: "Bold", prefix: "**", suffix: "**", default: "bold" },
    { icon: "italic", tip: "Italic", prefix: "_", suffix: "_", default: "italic" },
    { icon: "strikethrough", tip: "Strikethrough", prefix: "~~", suffix: "~~", default: "text" },
    { icon: "font-size", tip: "Heading", prefix: "### ", suffix: "", default: "Heading" },
    { icon: "code", tip: "Code", prefix: "`", suffix: "`", default: "code" },
    { icon: "block", tip: "Code Block", prefix: "\n```\n", suffix: "\n```\n", default: "code" },
    { icon: "minus", tip: "Quote", prefix: "> ", suffix: "", default: "quote" },
    { icon: "unordered-list", tip: "Bullet List", prefix: "- ", suffix: "", default: "item" },
    { icon: "ordered-list", tip: "Numbered List", prefix: "1. ", suffix: "", default: "item" },
    { icon: "link", tip: "Link", prefix: "[", suffix: "](url)", default: "text" },
    { icon: "picture", tip: "Image", prefix: "![", suffix: "](url)", default: "alt text" },
    {
      icon: "table",
      tip: "Table",
      prefix: "\n| Col 1 | Col 2 | Col 3 |\n| --- | --- | --- |\n| ",
      suffix: " |  |  |\n",
      default: "",
    },
    { icon: "line", tip: "Divider", prefix: "\n---\n", suffix: "", default: "" },
  ];

  constructor(private markdownService: MarkdownService) {}
  ngOnInit(): void {
    if (this.modalData) {
      this.description = this.modalData.description ?? "";
      this.isEditing = true;
    }
    this.editingContent = this.description;
    this.renderMarkdown(this.description);
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["description"] && !changes["description"].firstChange) {
      if (this.isEditing) {
        return;
      }
      this.editingContent = this.description;
      this.renderMarkdown(this.description);
    }
  }

  save(): void {
    this.description = this.editingContent;
    this.descriptionChange.emit(this.description);
    this.renderMarkdown(this.description);
    this.isEditing = false;
  }

  insert(action: { prefix: string; suffix: string; default: string }): void {
    const textarea = this.textareaRef.nativeElement;
    const selectionStart = textarea.selectionStart;
    const selectionEnd = textarea.selectionEnd;
    const selectedText = this.editingContent.substring(selectionStart, selectionEnd) || action.default;

    const textBefore = this.editingContent.substring(0, selectionStart);
    const textAfter = this.editingContent.substring(selectionEnd);
    this.editingContent = textBefore + action.prefix + selectedText + action.suffix + textAfter;
    this.renderMarkdown(this.editingContent);

    requestAnimationFrame(() => textarea.focus());
  }

  renderMarkdown(text: string): void {
    this.renderedDescription = text?.trim() ? this.markdownService.parse(text) : "";
  }
}

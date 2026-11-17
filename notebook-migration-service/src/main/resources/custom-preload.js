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

// Setup that must be in place before the notebook loads. main.js waits for this
// module, while custom.js loads alongside the notebook and can miss its events.
define([
  "base/js/events",
  "notebook/js/cell",
  "notebook/js/keyboardmanager",
  "notebook/js/kernelselector",
  "notebook/js/textcell",
], function (events, cell, keyboardmanager, kernelselector, textcell) {
  // The embedded notebook is a read-only reference view of the generated workflow.
  // Notebook re-derives each editor's readOnly from is_editable() on every focus.
  cell.Cell.prototype.is_editable = function () {
    return false;
  };

  // Focusing an editor re-enables the keyboard manager, which would bring back
  // shortcuts such as Shift+Enter to run a cell.
  keyboardmanager.KeyboardManager.prototype.enable = function () {};
  events.on("notebook_loaded.Notebook", function () {
    Jupyter.keyboard_manager.disable();
  });

  // The panel never runs cells, so a notebook whose kernel is not installed (an R
  // notebook, for example) opens without one instead of prompting for a kernel.
  kernelselector.KernelSelector.prototype._spec_not_found = function () {
    this.events.trigger("no_kernel.Kernel");
  };

  // Keep markdown cells rendered: overriding unrender() stops a double-click (or
  // Enter) from dropping a markdown cell into its editable source view.
  textcell.MarkdownCell.prototype.unrender = function () {};
});

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

// The Texera app origin. The "__TEXERA_ORIGIN__" placeholder is substituted at
// container startup by start-texera-jupyter.sh from the TEXERA_ORIGIN env var
// (defaults to http://localhost:4200), so deployments under a real hostname work
// without editing this file.
const TEXERA_ORIGIN = "__TEXERA_ORIGIN__";

// Use Jupyter's event system to ensure the notebook is fully loaded
require(["base/js/events"], function (events) {
  events.on("kernel_ready.Kernel", function () {

    // Attach click event listener to cells. kernel_ready.Kernel fires on every
    // kernel (re)start, so remove any previously bound handler first to avoid
    // stacking duplicate listeners that would post N messages per click.
    $("#notebook-container").off("click", ".cell").on("click", ".cell", function (event) {
      const cell = $(this);
      const index = $(".cell").index(cell);
      const cellContent = cell.find(".input_area").text();

      // Get the UUID from the cell's metadata, or use "N/A" if it doesn't exist
      const cellUUID = Jupyter.notebook.get_cell(index).metadata.uuid || 'N/A';

      // Send a message to the parent window (Texera app)
      window.parent.postMessage(
        { action: "cellClicked", cellIndex: index, cellContent: cellContent, cellUUID: cellUUID },
        TEXERA_ORIGIN
      );
    });
  });
});

// Listen for messages from the Texera app (or parent window)
window.addEventListener("message", function (event) {
  // Verify the message origin
  if (event.origin !== TEXERA_ORIGIN) {
    console.warn("Message received from unrecognized origin:", event.origin);
    return;
  }

  if (event.data.action === "triggerCellClick") {
    const operatorCellUUIDs = event.data.operators || [];

    if (!operatorCellUUIDs.length) {
      console.error("No valid operator UUIDs provided in the message.");
      return; // Exit if no UUIDs are provided
    }

    operatorCellUUIDs.forEach((cellUUID) => {
      // Search for the cell by UUID
      const allCells = Jupyter.notebook.get_cells();
      const targetCell = allCells.find((cell) => cell.metadata.uuid === cellUUID);

      if (targetCell) {
        const cellIndex = Jupyter.notebook.find_cell_index(targetCell);

        // Scroll to and highlight the cell
        let cell = document.querySelectorAll(".cell")[cellIndex];
        if (cell) {
          cell.scrollIntoView({ behavior: 'smooth', block: 'center' });
          cell.classList.add("highlighted");

          // Remove the highlight after 3 seconds
          setTimeout(() => {
            cell.classList.remove("highlighted");
          }, 3000);
        } else {
          console.error(`Cell not found in the DOM for index ${cellIndex}.`);
        }
      } else {
        console.error(`No cell found with UUID: ${cellUUID}`);
      }
    });
  } else {
    console.warn("Received unknown action:", event.data.action);
  }
}, false);

// Add custom CSS for highlighted cells
const style = document.createElement('style');
style.innerHTML = `
  .cell.highlighted {
    background-color: lightyellow;
  }
`;
document.head.appendChild(style);

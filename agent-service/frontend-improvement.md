So now I have a few things to improve on the pure frontend UI. Section 1 is about operators.

# Section 1: Operator Style Improvement
Currently, each operator shows the operator type at the top and the description at the bottom. I want to make the following changes to the operator style:

1. Operator Type
   Instead of showing the operator type on top, I want to show it within the operator box and below the operator's icon.

2. Operator ID
   For the top text of the operator, I want to show the operator's ID instead of the type.

Also, for the bottom operator's frontend display names, currently the width seems to be unlimited. Basically, all the tags are in one line; I want to make the text multiple lines by fixing the width of the bottom's customDisplayName.

# Section 2: Action version display

On the agent chat panel, we have a tree of agent actions where a user can click on a tree to view the versions. I want to make a few improvements to that interface.

The current problem is that the version tree is just a tree, but each version and each action actually has an ID containing a timestamp that corresponds to the chronological order of those actions. I would like to implement the following changes:

1. Add a vertical axis of time in that tree panel.
   (a) There should be multiple nodes on that axis.
   (b) Each node should be horizontally aligned with its corresponding action.
   (c) Next to each node, show a time label in the format of HH:MM:SS (all in two digits).

2. Refine the tree layout.
   (a) Ensure each node in the tree is horizontally aligned with the corresponding node on the time axis.
   (b) Ensure the distance between each dot on the time axis is constant.

With these changes, we can view the tree of actions to get the version information while also seeing the relationship between those versions and the time they occurred.

# Section 3: Action preview

Currently, users can click on the action node to view the data workflow and highlight the delta between two versions. We also show a panel on the agent chat panel where the user can choose to apply that action to go back to that version.

I want to improve the UI logic in two ways:

1. Reposition the "go back" decision panel:
   I want to move the panel for choosing whether to go back to a version from the chat panel to right below the operator representing the delta. If we have a new operator, I will show that "go back or not" panel directly below the operator that is being modified, deleted, or added. This way, users don't have to navigate back to the chat panel to make this decision.

2. Update the highlight style:
   Currently, we use a green or red color highlight (shading) on the operator that gets modified or added. I want to replace this shaded area with a borderline instead.
   (a) The highlight should have no fill color, only a border.
   (b) The border should be a dashed line.
   (c) We will maintain the same color scheme as the current version: the line should be green when an operator is added and red when an operator is modified.

Essentially, we are replacing the area shading with a dashed borderline while keeping the existing color logic.

# Section 4: simplify the feedback

Currently when hovering over an operator, there will be an icon where I can click and open and send message to agent. It also
shows to me the list of ReAct steps and it also highlight the upstream on the workflow, and it also shows a number on the top left
regarding the step id. It is too complex! I want to remove the upstream highlight, the step id, and also the react step display in that panel. 

That panel should simply be a input text area, with the agent drop down, and that's id. It is a pure frontend simplification


# Section 5: simplify the operator detail expansion

## 1. change the toggling location
Currently, the operator's detail is toggled by a button on the chat panel, but I want to change that behavior.

I want that toggle to be at the top of the operator menu instead. When I toggle it on, all the operators will use the details box layout. This way, it's not a behavior bundled to the agent; it's a behavior bundled to the whole workspace.

There are a few such buttons on the workspace panel:
1. Some toggle the region
2. Some toggle the detail
See menu.component.html for this

I want you to move the functionality and the toggle there. This will simplify the logic and make the behavior bundled with the workspace rather than the agent.

I also want to make sure that this toggle to control the result info fetched from the agent. Specifically, I want to use the toggle to control that because on the expansion panel, we are showing some operator information from the agent (for example, the shape in and out and that kind of stuff). I still want that to show on the detail expansion panel, and the relocation of this toggle should not affect that part.

## 2. change the operator expanded panel's content layout

For this change, I want to adjust the layout of the expanded detail panel. Specifically, the current layout consists of two areas:

1. The first area shows the operator icon, operator's type, and the in and out information, all arranged in the same horizontal layout.
2. The operator's detail, like important propertioes is then shown below this section.

I want to change this layout by:
1. move the in & out display to be above the port. e.g. for input info, show it at the corresponding input port, for output info, show it at the corresponding output port
2. Once we move that display out, I want to change the layout to be one section:
   (a) On the left is the operator icon and operator type
   (b) On the right are the key properties that vary based on operators

# Section 6: polish the feedback feature
Currently, each operator has a small button at the top right where the user can click it to open a panel and provide feedback. I want to display sample records within that panel and make a few specific improvements:

1. Remove the blue header from the panel. It is redundant, so we don't need to show it anymore; just show the area directly.

2. Add a new section above the text input area to display the sample records as a table.
   (a) The sample records come from the operator info, which should contain an array of JSON with the row index and the sample records for each field.
   (b) Note that there is a special column in each record called ___row_index___ (starting with an underscore) that shows the row index; please render that as well.
   (c) Since we use truncation (keeping the front and the end), there will be some ellipses between records. You should show these ellipses when you detect that the row index is jumping to the end.

3. Please make sure sample records come purely from the operator info sent from the agent's WebSocket, and use that as a ground truth to render information.
Please implement these suggestions.

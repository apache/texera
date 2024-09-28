<h1 align="center">IcedTea - Efficient and Responsive Time-Travel Debugging in Dataflow Systems.</h1>

This repo is an implementation of IcedTea built on top of an open-source data processing platform, [Texera](https://github.com/texera/texera).

## Prerequisites:
1. Java 11
2. Node JS LTS version
3. SBT
4. Yarn

You can follow [this guide](https://github.com/Texera/texera/wiki/Getting-Started) to install all the requirements.

## To run the project:
First, install frontend packages by running this command in bash:
```agsl
core/scripts/build.sh
```

Then, start both frontend and backend by running this command:
```agsl
core/scripts/deploy-daemon.sh
```

## Instructions for Using the Time-Travel Feature

### 1. Upload a Workflow
Before using the time-travel feature, you need to create or upload a workflow. To get started, I’ve included 5 sample workflows (Q1.json - Q5.json) used in the user study, which you can find in this repository.

- Step 1: Click the Upload button on the platform to upload one of the sample workflows.

<img width="238" alt="截屏2024-09-28 上午1 02 29" src="https://github.com/user-attachments/assets/c20170ec-0e15-4300-b6e2-a287c70fac81">

### 2. Set an Operator as "Interesting"
Once the workflow is uploaded, open it in the Workflow Editor.

- Step 2: Right-click on any operator in the workflow and choose Set as InterestingOperator to mark it for further monitoring.

<img width="447" alt="截屏2024-09-28 上午1 11 25" src="https://github.com/user-attachments/assets/43abb43b-1d70-4a75-994f-be0707a1a96e">


### 3. Run the Workflow
After selecting the "Interesting" operator, you can run the workflow.

- Step 3: Click the Run button at the top-right of the editor to start the execution.

- Optional: Enable Auto-Interaction by checking the box next to the run button. You can also adjust the interval for the auto-interaction.

<img width="406" alt="截屏2024-09-28 上午12 57 52" src="https://github.com/user-attachments/assets/70d88bd1-fa13-4a9f-bb3a-7cf62b386ef0">



### 4. Pause and Interact with the Workflow
If you are manually interacting with the workflow during execution, you can pause it to explore a snapshot of the operator’s state.

- Step 4: Click the Pause button to view a tuple-consistent state. Then, click the Interaction button to remember the current state so you can return to it later using the time-travel feature.

<img width="767" alt="截屏2024-09-28 上午12 59 23" src="https://github.com/user-attachments/assets/1df513b4-a8ee-435f-8f14-7c411597bf0f">



### 5. Use the Time-Travel Feature
After the workflow execution finishes, you can access the time-travel feature to revert the workflow to any previous state with an interaction.

- Step 5: In the left panel, click the Clock button to open the time-travel feature.

- Step 6: You will see a list of past executions. Select an execution with interactions and click the interaction to revert the workflow back to that state.

<img width="556" alt="截屏2024-09-28 上午12 58 41" src="https://github.com/user-attachments/assets/5a63c587-cc41-4f1b-b070-8166e15a9825">



### 6. Resume and Step Through the Workflow
After reverting the workflow to the chosen interaction point, you can resume stepping through it.

- Step 7: Right-click an operator and choose Step to take the next action in the workflow.

<img width="434" alt="截屏2024-09-28 上午12 59 00" src="https://github.com/user-attachments/assets/af493920-4d25-4f8f-b2ad-b99ce98382da">


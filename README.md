<h1 align="center">Udon: Efficient Debugging of User-Defined Functions in Big Data Systems with Line-by-Line Control</h1>

<img src="core/new-gui/src/assets/logos/full_logo_small.png" alt="texera-logo" width="96px" height="54.5px"/> Udon builds on top of Texera, a collaborative data analytics workflow system.

***

## Before we start….

Udon is a UDF debugger built on top of the data analytical workflow system
called [Texera](https://github.com/Texera/texera). This repo was branched out from Texera back in 07/2023, to maintain
experimental codebase for Udon. We kept this repo untouched so that it remains the state as when we conducted the
experiments. The instructions are a bit manual as our product is a debugger, which requires many manual usages given its
nature.

For the past 1 year, Texera (with Udon as a major feature) as evolved greatly, in terms of UI design, workflow
executions, and as well as Udon debugger features. The most recent Udon in Texera has a nice graphical user interface (
GUI) to set breakpoints and conditions, as well as buttons to skip, retry a faulty tuple. If interested, please feel
free to checkout https://github.com/Texera/texera/tree/yicong-udon branch to view the latest features! **The GUI is much
easier to use than CLI!**

The remaining of the guide will be structured in the following sections:

1. Get Started
2. Udon Components Overview
3. Play with Udon (in production)
4. Play with Udon (for experiments)
5. Reproduce experiments

***

## Get Started

<details>
  <summary>Prepare Env</summary>

1. Clone this repo: `git clone https://github.com/Texera/Udon`
2. Prepare backend:
    1. Get into `core/amber` folder
    2. Install JDK@11
    3. Install sbt@1.5.5
    4. (Optional) Test building the project with `sbt clean package`
3. Prepare frontend:
    1. Get into `core/new-gui` folder
    2. Install node@18LTS
    3. Install yarn@1.22.22
    4. Install dependencies: `yarn install`
4. Prepare Python
    1. Install python@3.9
    2. Create a Python virtualenv `python3.9 -m venv venv`
    3. Checkout to the new virtual env: `source venv/bin/activate`
    4. **Important** Find out your Python executable path: `which python` and enter it into `core/amber/src/main/resources/python_udf.conf` -> `path` field
    5. Install Python dependencies: `pip install -r core/amber/requirements.txt`
    6. Some of the tests workflows require more dependencies:
        1. `pip install nltk`
        2. `pip install spacy~=3.2.6` , then `python -m spacy download en_core_web_sm`\
        3. `pip install Pillow`
    

</details>

<details>
  <summary>Start Texera (with Udon enabled)</summary>

1. Get back into `core/`
2. Start the service by executing `./scripts/deploy-daemon.sh`, which will start a web service at port 8080 and a
   worker instance.
3. In the browser, navigate to http://localhost:8080 to access the Texera service. We can run workflows and Udon tests
   here.
4. To terminate, execute `./scripts/terminate-daemon.sh` which will stop all services and release all ports.
</details>


***

## Udon components
<details>
  <summary>Two-thread execution model</summary>
Udon executes Python UDFs with a two-thread model.

- [Data processing thread](https://github.com/Texera/Udon/blob/master/core/amber/src/main/python/core/runnables/data_processor.py)
- [Control processing thread](https://github.com/Texera/Udon/blob/master/core/amber/src/main/python/core/runnables/main_loop.py)
</details>


<details>
  <summary>Debug Manager</summary>
Udon manages the integrated debugger with
a [DebuggerManager](https://github.com/Texera/Udon/blob/master/core/amber/src/main/python/core/architecture/managers/debug_manager.py).

</details>

<details>
  <summary>SingleBlockingIO</summary>
The customized IO that supports:
-Transferring one message at a time, from producer to consumer.
-Blocking readline() when no message is available.
-Whenever being blocked, it switches between producer and consumer.

With the `SingleBlockingIO`, we achieved the context switch between the control processing (aka, `main_loop`) and data
processing threads.
</details>

<details>
  <summary>Debugger Configurations</summary>
There are the following configurations:

| Parameter name              | Type | Default Value | Usage                                                             |
|-----------------------------|------|---------------|-------------------------------------------------------------------|
| PAUSE_ON_HITTING_BREAKPOINT | Bool | False         | Let the debugger halt the execution when hitting a breakpoint.    |
| PAUSE_ON_SETTING_BREAKPOINT | Bool | False         | Let the debugger halt the execution when a new breakpoint is set. |
| OP1_ENABLED                 | Bool | False         | Enables OP1, to hot-swap UDF code                                 |
| OP2_ENABLED                 | Bool | False         | Enables OP2, to pull up predicates                                |
</details>

<details>
  <summary>Debug-related Messages (Command and Event)</summary>

- `DebugCommand`: a message from the user to the debugger, instructing an operation in the debugger, expecting no
  response from the debugger.
- `DebugEvent`: a message from the debugger to the user, which could be a reply to a DebugCommand or an event triggered
  in the debugger (e.g., hit a breakpoint, exception).

The global picture of the debug-related messages exchange life cycle is shown below:
<img width="979" alt="Screenshot 2022-12-05 at 21 23 35" src="https://user-images.githubusercontent.com/17627829/205823569-00e1a53a-90f5-43c1-a686-77187343d4c7.png">

</details>

***

## Play with Udon (in production)

Udon supports actual control messages from the controller, which requires the user to manually input debug comments from
the Python UDF console.
To experience the full debugger experience in action, please enable `PAUSE_ON_HITTING_BREAKPOINT` option.

<details>
  <summary>Some sample debug commands:</summary>

- Set a breakpoint: `b 20, "hello" in tuple_['text']`
- Step (after hitting a breakpoint): `n`
- Continue (from a breakpoint): `c`
- Clear breakpoint: `clear 1` (where 1 is the breakpoint number); or `clear` for all
- List code: `ll`
</details>

***

## Play with Udon (for experiments)

For demonstration and experiment purposes, we simulate debug instructions inside the control
processing thread. We provided a method `simulate_debug_command` to send arbitrary debug command to the target Python
UDF worker.

### pdb debug commands

Example to add a `setting breakpoint` command at line 20 of the UDF code, with some conditions:

```python
# in main_loop.py:127
self.simulate_debug_command(
    "b 20, 'Udon' in tuple_['text'] and 'welcome' in tokens"
)
```

Udon supports all [pdb commands](https://docs.python.org/3/library/pdb.html)
except
for [`jump`](https://www.google.com/search?client=safari&rls=en&q=pdb+commands&ie=UTF-8&oe=UTF-8), [
`interact`](https://docs.python.org/3/library/pdb.html#pdbcommand-interact)
and [`quit`](https://docs.python.org/3/library/pdb.html#pdbcommand-quit).

### Additional debug commands

In addition to the standard debug commands, Udon also supports state transfer commands, including `ss` (store
state), `rs` (request state), and `as` (append state).
Example to add a `request state` command at line 20 to request the upstream Python worker's state at line 22:

```python
# in main_loop.py:132
self.simulate_debug_command(
    "rs 20 22 tokens "
    "PythonUDFV2-operator-8c277eca-adb7-4b4f-866c-3e8950535ef1-main"
)
```

For active transfer:

- AppendState. `as lineno state_name` Append the line state specified by the state lineno and state name to downstream
  operators.

For passive transfer:

- StoreState. `ss lineno state_name` Store the line state specified by the state lineno and state name for future
  reference.
- RequestState. `rs lineno state_lineno state_name target_worker_id` Request the state specified by the state lineno and
  state name from the target upstream worker, when processes to lineno.

***

## Reproduce Experiments:

<details>
  <summary>Example workflows for experimental purpose</summary>

- Workflows (W1 - W6) and datasets used in the experiments are available in [core/experiment-related](core/experiment-related/) directory.

</details>

<details>
  <summary>General Steps for One Experiment</summary>

1. On the UI, import one workflow.json into the workspace at one time.
2. Change the source operators to scan files from your own path on the UI.
    1. Click a source operator (e.g., CSV Scan), the property panel will show up on the right-hand side.
    2. Modify the `file path` field to your local file path.
3. Turn on the corresponding simulated debug command in `main_loop.py` (you do not need to restart the server. the
   python code changes are read upon every execution). See comments in code for details. Please make sure to turn on one
   simulated debug command at one time.
4. Submit the workflow to execute by clicking the blue Run button on the UI.
5. You can find execution time report from the logs under `core/log/` , or from the Python UDF console.
    1. Console: click the Python UDF operator on the UI, a console will show up on the bottom-left. Time will be printed
       out there.
    2. Logs:
        1. `tail -f core/log/*.log | grep "total time in eval:"`
        2. `tail -f core/log/*.log | grep "total time of operator:"`

</details>


<details>
  <summary>Detailed Instructions for Each Experiment</summary>

- To reproduce Table 1 & Figure 18, please set all four configurations to False. Then load W1-W5 and turn on the
  simulated debug command accordingly.
- To reproduce Figure 19, please set `OP1_ENABLED` and `OP2_ENABLED` to True separately, this will enable the two
  optimizations accordingly. Please enable both options to have both optimizations take effect. Then load W1-W5 and turn
  on the simulated debug command accordingly.
- To reproduce Figure 20, load the W3 and feel free to generate larger datasets with TPC-H.
- To reproduce Figure 21, load the W3 and change the number of workers on the Python UDF operator on the UI.
    - Click on a Python UDF operator, the property panel will show up on the right-hand side.
    - Modify the `Workers` field to any positive integer. Recommended range: 1 - 8 and it depends on your CPU.
- To reproduce Figure 23, Figure 24 & Figure 26, load the W6 and turn off all simulated breakpoints, turn on the
  simulated debug commands for `ss` (store state), `rs` (request state), and `as` (append state) as needed.
</details>

The collected results can be input to [Udon_Experiment_Figures.ipynb](core/experiment-related/Udon_Experiment_Figures.ipynb) to generate Figures.
***

## Acknowledgements

This project is supported by the <a href="http://www.nsf.gov">National Science Foundation</a> under the
awards [III 1745673](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1745673), [III 2107150](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2107150),
AWS Research Credits, and Google Cloud Platform Education Programs.

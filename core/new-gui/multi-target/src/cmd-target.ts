import { BuilderOutput, BuilderContext, createBuilder, Target } from '@angular-devkit/architect';
import { ChildProcess, spawn, execSync } from 'child_process';
import { platform, kill } from 'process';
import { JsonObject } from '@angular-devkit/core';

// cmd-target logs cmd's output with a header that gets truncated to CMD_LENGTH characters
//eg: "echo foo" -> "(echo foo): foo", "echo supercalifragilisticexpialidocious" -> "(ech...ous): supercalifragilisticexpialidocious"
const CMD_LENGTH = 20;

//static array of CmdTask. gets added to when creating a task. used to lookup tasks to kill.
let liveTasks: CmdTask[] = [];

/** 
 * properties:  
 *  - **cmd**: *str* command as you would copy-paste into a shell  
 *  - **daemon**: *bool* true to run without logging cmd's output  
 *  - **detached**: *bool* true to detach cmd's process. builder will resolve successfully immediately and cmd process will continue even if nodejs stops  
 *  - **kill**: *bool* true to kill process with matching cmd that was run previously.
 *  - **killChildren**: *bool* true to also kill process descendants (processes launched by cmd) 
*/
interface Options extends JsonObject {
  cmd: string;
  daemon: boolean;
  detached: boolean;
  kill: boolean;
  killChildren: boolean;
}

/**
 * properties:
 *  - **process**: *ChildProcess* process object
 *  - **cmd**: *str* command as you would copy-paste into a shell
 */
interface CmdTask {
  process: ChildProcess,
  cmd: string
}


type PromiseFunc = (value?: BuilderOutput | PromiseLike<BuilderOutput> | undefined) => void

export default createBuilder<Options>(cmdBuilder);

function cmdBuilder(options: Options, context: BuilderContext): Promise<BuilderOutput> {
  return new Promise<BuilderOutput>(async (resolve: PromiseFunc, reject: PromiseFunc) => {
    let builderOutput: BuilderOutput = options.kill ? killTarget(options, context) : await execTarget(options, context);
    builderOutput.success ? resolve(builderOutput) : reject(builderOutput);
  });
}

/**
 * 
 * @param cmd string to clamp the length of
 * @param length number of characters to clamp to
 * @returns clamped cmd string
 */
function clampCmd(cmd: string, length: number): string {
  if (cmd.length <= length) {
    return cmd
  } else {
    return cmd.substring(0, Math.floor(length / 2)) + "..." + cmd.substring(cmd.length - Math.ceil(length / 2) - 1, cmd.length)
  }
}

/**
 * 
 * @param parentPID pid of parent process
 */
function killDescendants(parentPID: number): void {
  if (platform == "win32") {
    // On windows, use wmic to lookup processes by PPID
    // and using a Stack, "recursively" discover all descendants, and descendants of descendants, etc
    var descendantPids: Set<number> = new Set();
    var pidStack: number[] = [parentPID];
    while (pidStack.length > 0) {
      //@ts-ignore pidstack.pop() won't return null since it isn't empty cause it's length isn't zero'
      var currentPid: number = pidStack.pop();
      if (!descendantPids.has(currentPid)) {
        descendantPids.add(currentPid);
        var wmicOutput: string = execSync(`wmic process where (ParentProcessId=${parentPID}) get ProcessId`).toString(); // executes wmic and returns str of output
        var lines: string[] = (wmicOutput.match(/\S+/g) || []); // matches non-whitespace, forming array of "words"
        lines.shift(); // remove 0th item, which would be the column label "ProcessId" if we were in a shell
        var childPids: number[] = lines.map(x => parseInt(x));
        childPids.forEach((pid: number) => {
          pidStack.push(pid);
        });
      }
    }
    descendantPids.delete(parentPID);
    descendantPids.forEach((pid: number) => {
      kill(pid);
    });
  } else if (platform == "linux") {
    execSync(`kill -TERM -${parentPID}`); //kills parentPID assuming it's a linux process group. <3 linux so ez
  } else {
    // other possibilities are AIX|Darwin|FreeBSD|OpenBSD|SunOS
    console.warn(`Warning: killing process descendants currently only supported on windows and linux. Please check manually if it worked.`) // narrator: it didn't
  }
}

/**
 * 
 * @param cmd command as you would copy-paste into a shell
 * @param killChildren true to also kill process descendants
 * @returns true if successful, false if couldn't find CmdTask
 */
function killCmdTask(cmd: string, killChildren: boolean): boolean {
  let index = liveTasks.findIndex(element => element.cmd == cmd);
  if (index != -1) {
    if (killChildren) {
      killDescendants(liveTasks[index].process.pid);
    }
    liveTasks[index].process.kill('SIGINT');
    liveTasks.splice(index, 1); // remove livetask (since it's been killed)
    return true
  } else {
    return false
  }
}

/**
 * 
 * @param cmd command as you would copy-paste into a shell
 * @param loggerPrefix prepended to cmd output (to label it)
 * @param options instance of Options
 * @param context instance of BuilderContext
 * @returns created CmdTask, with process already running
 */
function execCmdTask(cmd: string, loggerPrefix: string, options: Options, context: BuilderContext): CmdTask {
  const child: ChildProcess = spawn(cmd, [], { stdio: options.daemon ? 'ignore' : 'pipe', detached: options.detached, shell: true });

  if (!options.daemon) {
    // @ts-ignore: stdout won't be null since not running as daemon (childprocess's stdio = 'pipe')
    child.stdout.on('data', (data) => {
      context.logger.info(loggerPrefix + data.toString());
    });
    // @ts-ignore: stderr won't be null since not running as daemon (childprocess's stdio = 'pipe')
    child.stderr.on('data', (data) => {
      context.logger.error(loggerPrefix + data.toString());
    });
  }
  return { process: child, cmd: cmd };
}

/**
 * 
 * @param options instance of Options
 * @param context instance of BuilderContext
 * @returns BuilderOutput. BuilderOutput.success == true if successful
 */
function killTarget(options: Options, context: BuilderContext): BuilderOutput {
  context.logger.info(`Killing ${options.daemon ? "daemon " : ""} '${options.cmd}'`);
  if (!killCmdTask(options.cmd, options.killChildren)) {
    context.logger.warn(`Couldn't find/kill cmd '${options.cmd}'. it may not have been executed or already terminated`);
    return { success: false, target: context.target as Target }
  }
  return { success: true, target: context.target as Target }
}

/**
 * 
 * @param options instance of Options
 * @param context instance of BuilderContext
 */
function execTarget(options: Options, context: BuilderContext): Promise<BuilderOutput> {
  context.reportStatus(options.cmd);
  context.logger.info(`Executing ${options.daemon ? "as daemon " : ""} '${options.cmd}'`);

  const prefix: string = `(${clampCmd(options.cmd, CMD_LENGTH)}):`
  var task = execCmdTask(options.cmd, prefix, options, context);
  liveTasks.push(task);

  if (options.detached) {
    return Promise.resolve({ success: true, target: context.target as Target });
  } else {
    return new Promise<BuilderOutput>((resolve, reject) => {
      task.process.on('close', code => {
        let index = liveTasks.findIndex(element => element == task);
        if (index != -1) {
          liveTasks.splice(index, 1); // remove livetask (since it terminated)
        }
        if (code === 0) {
          context.reportStatus(`${prefix} successfully terminated`);
          context.logger.info(`${prefix} successfully terminated)`);
          resolve({ success: true, target: context.target as Target });
        } else {
          context.reportStatus(`${prefix} terminated (code ${code})`);
          context.logger.fatal(`${prefix} terminated (code ${code})`);
          resolve({ success: false, target: context.target as Target });
        }
      });
    })
  }
}
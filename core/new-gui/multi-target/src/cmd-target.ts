import { BuilderOutput, BuilderContext, createBuilder, Target } from '@angular-devkit/architect';
import { ChildProcess, spawn, execSync } from 'child_process';
import { platform, kill } from 'process';
import { JsonObject } from '@angular-devkit/core';

const CMD_LENGTH = 20;
let liveTasks: CmdTask[] = [];

interface Options extends JsonObject {
  cmd: string;
  daemon: boolean;
  detached: boolean;
  kill: boolean;
  killChildren: boolean;
}

interface CmdTask {
  process: ChildProcess,
  cmd: string
}

type PromiseFunc = (value?: BuilderOutput | PromiseLike<BuilderOutput> | undefined) => void

export default createBuilder<Options>((options, context) => {
  return new Promise<BuilderOutput>(async (resolve: PromiseFunc, reject: PromiseFunc) => {
    if (options.kill) {
      killTarget(options, context, resolve, reject);
    } else {
      execTarget(options, context, resolve, reject);
    }
  });
});

function clampCmd(cmd: string, length: number) {
  if (cmd.length <= length) {
    return cmd
  } else {
    return cmd.substring(0, Math.floor(length / 2)) + "..." + cmd.substring(cmd.length - Math.ceil(length / 2) - 1, cmd.length)
  }
}

function killDescendants(parentPID: number) {
  if (platform == "win32") {
    var descendantPids: Set<number> = new Set();
    var pidStack: number[] = [parentPID];
    while (pidStack.length > 0) {
      //@ts-ignore pidstack.pop() won't return null since it isn't empty cause it's length isn't zero'
      var currentPid: number = pidStack.pop();
      if (!descendantPids.has(currentPid)) {
        descendantPids.add(currentPid);
        var wmicOutput: string = execSync(`wmic process where (ParentProcessId=${parentPID}) get ProcessId`).toString();
        var lines: string[] = (wmicOutput.match(/\S+/g) || []);
        lines.shift();
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
  } else if (platform == "linux"){
    execSync(`kill -TERM -${parentPID}`);
  } else {
    // other possibilities are AIX|Darwin|FreeBSD|OpenBSD|SunOS
    console.warn(`Warning: killing process descendants currently only supported on windows and linux. Please check manually if it worked.`)
  }
}

function killCmdTask(cmd: string, killChildren: boolean): boolean {
  let index = liveTasks.findIndex(element => element.cmd == cmd);
  if (index != -1) {
    if (killChildren) {
      killDescendants(liveTasks[index].process.pid);
    }
    liveTasks[index].process.kill('SIGINT');
    liveTasks.splice(index, 1);
    return true
  } else {
    return false
  }
}

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

function killTarget(options: Options, context: BuilderContext, resolve: PromiseFunc, reject: PromiseFunc) {
  context.logger.info(`Killing ${options.daemon ? "daemon " : ""} '${options.cmd}'`);
  if (!killCmdTask(options.cmd, options.killChildren)) {  
    context.logger.warn(`Couldn't find/kill cmd '${options.cmd}'. it may not have been executed or already terminated`);
  }
  resolve({ success: true, target: context.target as Target });
}

function execTarget(options: Options, context: BuilderContext, resolve: PromiseFunc, reject: PromiseFunc) {
  context.reportStatus(options.cmd);
  context.logger.info(`Executing ${options.daemon ? "as daemon " : ""} '${options.cmd}'`);

  const prefix: string = `(${clampCmd(options.cmd, CMD_LENGTH)}):`
  var task = execCmdTask(options.cmd, prefix, options, context);
  liveTasks.push(task);

  if (options.detached) {
    resolve({ success: true, target: context.target as Target });
  } else {
    task.process.on('close', code => {
      killCmdTask(options.cmd, options.killChildren);
      if (code === 0) {
        context.reportStatus(`${prefix} successfully terminated`);
        context.logger.info(`${prefix} successfully terminated)`);
        resolve({ success: true, target: context.target as Target });
      } else {
        context.reportStatus(`${prefix} terminated (code ${code})`);
        context.logger.info(`${prefix} terminated (code ${code})`);
        reject({ success: false, target: context.target as Target });
      }
    });
  }
}
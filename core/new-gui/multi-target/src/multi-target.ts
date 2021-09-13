import {
  BuilderContext,
  BuilderOutput,
  BuilderRun,
  createBuilder,
  ScheduleOptions,
  Target,
  targetStringFromTarget,
  targetFromTargetString,
} from "@angular-devkit/architect";
import { JsonObject, logging } from "@angular-devkit/core";

/**
 * properties:
 *  - **targets**: *str[]* list of build targets
 *  - **sequential**: *bool* run targets in order
 *  - **race**: *bool* run targets in parallel until one of them finishes first (then stop others)
 */
interface Options extends JsonObject {
  targets: string[];
  sequential: boolean;
  race: boolean;
}

/**
 * properties:
 *  - **totalTargets** *number*
 * [//]: #
 * methods:
 *  - **incrementTargetFails**()
 *  - **incrementTargetSuccesses**()
 *  - **getTargetFails**()
 *  - **getTargetSuccesses**()
 *  - **subscribeToFails**(fn: *(targetFails: number) => void*)
 *  - **subscribeToSuccesses**(fn: *(targetSuccesses: number) => void*)
 */
class TargetData {
  totalTargets: number;
  failedTargets: Target[];
  successfulTargets: Target[];

  private targetFails: number;
  private targetSuccesses: number;
  private failSubscribers: ((targetFails: number) => void)[] = [];
  private successSubscribers: ((targetSuccesses: number) => void)[] = [];

  constructor(totalTargets: number, targetFails: number, targetSuccesses: number) {
    this.totalTargets = totalTargets;
    this.targetFails = targetFails;
    this.targetSuccesses = targetSuccesses;
    this.failedTargets = [];
    this.successfulTargets = [];
  }

  /**
   * increments targetFails (+1)
   * @event Calls all failSubscribers as a side effect
   */
  incrementTargetFails() {
    this.targetFails += 1;
    this.failSubscribers.forEach(fn => fn(this.targetFails));
  }

  /**
   * increments targetSuccesses (+1)
   * @event Calls all successSubscribers as a side effect
   */
  incrementTargetSuccesses() {
    this.targetSuccesses += 1;
    this.successSubscribers.forEach(fn => fn(this.targetSuccesses));
  }

  getTargetFails(): number {
    return this.targetFails;
  }

  getTargetSuccesses(): number {
    return this.targetSuccesses;
  }

  /**
   *
   * @param fn gets called every incrementTargetFails()
   */
  subscribeToFails(fn: (targetFails: number) => void) {
    this.failSubscribers.push(fn);
  }

  /**
   *
   * @param fn gets called every incrementTargetSuccesses()
   */
  subscribeToSuccesses(fn: (targetSuccesses: number) => void) {
    this.successSubscribers.push(fn);
  }
}

export default createBuilder(multiBuilder);

/**
 *
 * @param options Options
 * @param context BuilderContext
 * @returns promise of builderOutput, resolves if build successful, rejects if not. Always produces a BuilderOutput either way.
 */
function multiBuilder(options: Options, context: BuilderContext): Promise<BuilderOutput> {
  // For help understanding builders/Architect, visit https://angular.io/guide/cli-builder.

  // multiBuilder takes a list of build targets (options.targets) and runs them.
  //
  // In order to run a build target, it must be scheduled run, and then it's output handled.
  //
  // scheduling (scheduleTarget) yields a promise of a BuilderRun (the target is scheduled when the promise resolves)
  //
  // running is automatic after a BuilderRun is scheduled. BuilderRun.result is a promise of BuilderOutput

  // const used to label/prefix logs
  const multiTargetStr: string =
    context.target === undefined ? "Anonymous Multi-Target Builder" : targetStringFromTarget(context.target);

  return new Promise<BuilderOutput>(async (resolve, reject) => {
    if (options.sequential && options.race) {
      const errorMsg = `(${multiTargetStr}) invalid config: options 'race: true' and 'sequential: true' are incompatible`;
      context.logger.fatal(errorMsg);
      reject({
        success: false,
        target: context.target as Target,
        error: errorMsg,
      });
    }

    // Each BuilderRun represents a builder successfully scheduled and running.
    // Just as this builder function returns a Promise<BuilderOutput>, each BuilderRun.result is the Promise<BuilderOutput> of that builder
    const builderRuns: Promise<BuilderRun>[] = [];
    // As each BuilderRun.result resolves/rejects, we increment targetData.
    const targetData = new TargetData(options.targets.length, 0, 0);

    // Subscribing to changes in targetData.targetFails lets us determine when to reject (this builder has failed)
    targetData.subscribeToFails((targetFails: number) => {
      if (!options.race) {
        const subTargetStr = targetStringFromTarget(targetData.failedTargets[targetData.failedTargets.length - 1]);
        const errorMsg = `(${multiTargetStr})-->(${subTargetStr}) failed!`;
        context.logger.fatal(errorMsg);
        reject({
          success: false,
          target: context.target as Target,
          error: errorMsg,
        });
      } else if (options.race && targetFails === targetData.totalTargets) {
        const errorMsg = `(${multiTargetStr}) failed: all sub-targets failed! (race: true).`;
        context.logger.fatal(errorMsg);
        reject({
          success: false,
          target: context.target as Target,
          error: errorMsg,
        });
      } else if (options.race && targetFails < targetData.totalTargets) {
        const subTargetStr = targetStringFromTarget(targetData.failedTargets[targetData.failedTargets.length - 1]);
        const errorMsg = `(${multiTargetStr})-->(${subTargetStr}) failed. Ignoring due to config (race: true) `;
        context.logger.warn(errorMsg);
      }
    });

    // Subscribing to changes in targetData.targetSuccesses lets us determine when to resolve (this builder has succeeded)
    targetData.subscribeToSuccesses((targetSuccesses: number) => {
      if (options.race || targetSuccesses === targetData.totalTargets) {
        resolve({ success: true, target: context.target as Target });
      }
    });

    // For each target, schedule it, and add hooks that increment targetData with each build fail/success.
    for (let i = 0; i < options.targets.length; i++) {
      const target = targetFromTargetString(options.targets[i]);

      builderRuns.push(scheduleTarget(target, context));
      // a target can fail to be scheduled if target doesn't exist or couldn't find module dependency.
      builderRuns[i].catch((error: any) => {
        targetData.failedTargets.push(target);
        targetData.incrementTargetFails();
      });
      // when a target is scheduled successfully, the builder that is scheduled can fail too:
      builderRuns[i].then((builderRun: BuilderRun) => {
        // ... for any reason, e.g. Cmd-target with cmd = "asdf" --(console)-> "'asdf' is not recognized as an internal or external command"
        builderRun.result.catch((error: any) => {
          targetData.failedTargets.push(target);
          targetData.incrementTargetFails();
        });
        // ... but if it succeeds, increment jobData
        builderRun.result.then((builderOutput: BuilderOutput) => {
          targetData.successfulTargets.push(target);
          targetData.incrementTargetSuccesses();
        });
      });
      // if sequential, await scheduleTarget scheduling a BuilderRun and said BuilderRun.result resolving into a BuilderOutput
      if (options.sequential) {
        await (
          await builderRuns[i]
        ).result;
      }
    }
  });
}

/**
 *
 * @param target Build Target to schedule
 * @param context Builder Context to schedule with
 * @returns Promise of a **BuilderRun** (resolves if scheduled successfully).
 * Each **BuilderRun** represents a builder successfully scheduled and running.
 * Each **BuilderRun.result** is the **Promise<BuilderOutput>** of that builder (resolves if builder runs successfully).
 */
function scheduleTarget(target: Target, context: BuilderContext): Promise<BuilderRun> {
  // @ts-ignore logger is actually Logger but was interfaced (as part of BuilderContext) into a LoggerApi.
  const opt: ScheduleOptions = { logger: <logging.Logger>context.logger };
  const overrides: JsonObject | undefined = undefined;
  return context.scheduleTarget(target, overrides, opt);
}

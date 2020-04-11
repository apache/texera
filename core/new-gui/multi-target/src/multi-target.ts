import { BuilderContext, BuilderOutput, BuilderRun, Target, ScheduleOptions, createBuilder, targetStringFromTarget, targetFromTargetString, scheduleTargetAndForget } from '@angular-devkit/architect';
import { JsonObject, logging} from '@angular-devkit/core';
import * as childProcess from 'child_process';
import { Observable } from '@angular-devkit/core/node_modules/rxjs';

interface Options extends JsonObject {
  targets: string[];
  sequential: boolean;
  race: boolean;
}

interface JobData extends Object {
  buildFails: number;
}

export default createBuilder(multiBuilder);

function multiBuilder( options: Options, context: BuilderContext): Promise<BuilderOutput> {
  return new Promise<BuilderOutput>(async (resolve, reject) => {
    let multiTargetStr: string = (context.target === undefined) ? "Multi-Target Builder" : context.target.project + ":" + context.target.target + ":" + context.target.configuration;
    
    if(options.sequential && options.race){
      context.logger.fatal(`(${multiTargetStr}) invalid config: options 'race:true' and 'sequential:true' are incompatible`);
      reject({success: false, target: context.target as Target})
    }
    
    let builderOutputs: Promise<BuilderOutput>[] = [];
    let builderRuns: Promise<BuilderRun>[] = [];
    let jobData: JobData = {buildFails: 0};
    
    for (var i: number = 0; i < options.targets.length; i++){
      builderRuns.push(scheduleTarget(options.targets[i],context));
      
      builderRuns[i].catch((error) => {
        context.logger.fatal(`(${multiTargetStr})-->(${options.targets[i]})failed!`)
        reject({success: false, target: context.target as Target, error: error.toString()})
      })
      
      builderRuns[i].then((builderRun: BuilderRun) => {
        builderOutputs.push(builderRun.result);
        builderRun.result.catch((builderOutput: BuilderOutput) => {
          let subTargetStr: string = (builderOutput.target === undefined) ? "Anonymous " + builderRun.info.builderName : builderOutput.target.project + ":" + builderOutput.target.target + ":" + builderOutput.target.configuration

          if(options.race){
            context.logger.warn(`(${multiTargetStr})-->(${subTargetStr}) failed! Ignoring due to config(race:true). `)
            jobData.buildFails += 1;
            if(jobData.buildFails >= options.targets.length){
              reject({success: false, target: context.target as Target});
            }
          } else {
            context.logger.fatal(`(${multiTargetStr})-->(${subTargetStr}) failed!`)
            reject(builderOutput);
          }
          
        });
      });
      if (options.sequential){
        await (await builderRuns[i]).result;
      } else if (options.race){
        builderRuns[i].then((builderRun: BuilderRun) =>{
          builderRun.result.then((builderOutput: BuilderOutput) => {
            context.logger.warn("resolving race")
            context.logger.warn(`${i} of ${options.targets.length} targets dispatched`);
            let stopPromises: Promise<void>[] = [];
            for(var j: number = 0; j < i; j++){
              builderRuns[j].then((runToStop: BuilderRun) => {
                if (runToStop != builderRun || true){
                  context.logger.warn(`${builderRun.info.builderName} stopped`)
                  stopPromises.push(runToStop.stop());
                }
              });
            }
            i = options.targets.length;
            Promise.all(builderRuns).then(() => {
              Promise.all(stopPromises).then(() => {
                context.logger.warn("resolved race");
                resolve({success: true, target: context.target as Target});
              });
            });
          });
        });
      }
      await builderRuns[i];
    }
    if (!options.race){
      Promise.all(builderRuns).then(() => {
        Promise.all(builderOutputs).then(() => {
          resolve({success: true, target: context.target as Target})
        });
      });
    }
  });
}

function scheduleTarget(targetStr: string, context: BuilderContext): Promise<BuilderRun>{
  let target = targetFromTargetString(targetStr);
  //@ts-ignore logger is actually Logger but was interfaced (as part of BuilderContext) into a LoggerApi.
  let opt: ScheduleOptions = {logger: <logging.Logger> context.logger};
  let overrides: JsonObject|undefined = undefined;
  return context.scheduleTarget(target,overrides,opt);
}
import { Injectable } from "@angular/core";
import Ajv from "ajv";
import { cloneDeep, has, indexOf, isEqual, merge, pickBy } from "lodash";
import { NzMessageService } from "ng-zorro-antd/message";
import { Observable, of, Subject } from "rxjs";

import { CustomJSONSchema7 } from "../../types/custom-json-schema.interface";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { first, map } from "rxjs/operators";
import { UserConfigService } from "../../../common/service/user/config/user-config.service";
import { asType, isType } from "../../../common/util/assert";

/**
 * Preset service enables saving and applying of Presets, which are objects
 * that represent a collection of settings that can be applied all together.
 * The intent is to allow a user to save settings pertaining to some texera object, and reuse them later
 *
 * Currently this mainly works for Operator properties. EX: for MysqlSource, users may reuse presets of address/port/username/database/table
 * Operator presets are determined by the presence of the 'enable-presets' in the individual properties of each OperatorSchema (see CustomJSONSchema7)
 * This service relies on DictionaryService for storage, which in turn requires the client to be logged in
 * @author Albert Liu
 */

/**
 * determines icon used by NzMessageService (the little notification that shows when a preset is saved)
 * success == green checkmark
 * error == red x-mark
 * warning == yellow !-mark
 * info == blue i-mark
 */
type AlertMessageType = "success" | "error" | "info" | "warning";

const PresetSchema: CustomJSONSchema7 = {
  type: "object",
  additionalProperties: {
    type: "string",
    pattern: "^\\S.*$",
  },
};

const PresetArraySchema: CustomJSONSchema7 = {
  type: "array",
  items: PresetSchema,
  uniqueItems: true,
};

export type Preset = { [key: string]: string | number | boolean };

export type PresetDictionary = {
  [Key: string]: Preset[];
};
@Injectable({
  providedIn: "root",
})
export class PresetService {
  private static DICT_PREFIX = "Preset"; // key prefix when storing data in dictionary service
  private static ajv = new Ajv();
  private static ajvStrip = new Ajv({ useDefaults: true, removeAdditional: true, strict: false }); // removes extra properties from an object that aren't described by schema
  private static isPreset = PresetService.ajv.compile(PresetSchema);
  private static isPresetArray = PresetService.ajv.compile(PresetArraySchema);

  public readonly applyPresetStream: Observable<{ type: string; target: string; preset: Preset }>;
  public readonly savePresetsStream: Observable<{ type: string; target: string; presets: Preset[] }>;

  private applyPresetSubject = new Subject<{ type: string; target: string; preset: Preset }>(); // event stream for applying presets to a target (usually type "operator" with specific operatorID as target)
  private savePresetSubject = new Subject<{ type: string; target: string; presets: Preset[] }>(); // event stream for saving preset[]s to a target (usually type "operator" an operatorType as target)

  constructor(
    private userConfigService: UserConfigService,
    private messageService: NzMessageService,
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService
  ) {
    this.applyPresetStream = this.applyPresetSubject.asObservable();
    this.savePresetsStream = this.savePresetSubject.asObservable();
    this.handleApplyOperatorPresets();
  }

  /**
   * broadcast applyPreset event, triggering any subscriber actions
   * By default, type "operator" applyPreset events trigger preset being applied to the targeted operator
   * @param type string, usually "operator"
   * @param target string, usually an operatorID
   * @param preset a subset of operator properties that will be applied
   */
  public applyPreset(type: string, target: string, preset: Preset) {
    this.applyPresetSubject.next({ type: type, target: target, preset: preset });
  }

  /**
   * broadcast savePresets event and also save preset to presetDict, which is a *view* (in the database sense) of DictionaryService's dictionary that only stores presets
   * @param type string, usually "operator"
   * @param target string, usualy operatorType
   * @param presets Preset[]
   * @param displayMessage message to display when saving presets
   * @param messageType see AlertMessageType, determines icon used in popup message
   */
  public savePresets(
    type: string,
    target: string,
    presets: Preset[],
    displayMessage?: string | null,
    messageType: AlertMessageType = "success"
  ) {
    if (presets.length > 0) {
      this.userConfigService.set(`${type}-${target}`, JSON.stringify(presets));
    } else {
      this.userConfigService.delete(`${type}-${target}`);
    }
    this.savePresetSubject.next({ type: type, target: target, presets: presets });
    this.displaySavePresetMessage(messageType, displayMessage);
  }

  /**
   * broadcast savePresets event and also save preset to presetDict, which is a *view* (in the database sense) of DictionaryService's dictionary that only stores presets
   * @param type string, usually "operator"
   * @param target string, usualy operatorType
   * @param presets Preset[]
   * @param displayMessage message to display when saving presets
   * @param messageType see AlertMessageType, determines icon used in popup message
   */
  public createPreset(
    type: string,
    target: string,
    preset: Preset,
    displayMessage?: string | null,
    messageType: AlertMessageType = "success"
  ) {
    this.userConfigService
      .fetchKey(`${type}-${target}`)
      .pipe(first())
      .subscribe(presetsString => {
        let presets = JSON.parse(presetsString ?? "[]") as Preset[];
        if (contains(presets, preset)) {
          throw new Error("attempting to create preset that already exists");
        }
        presets.push(preset);
        this.savePresets(type, target, presets, displayMessage, messageType);
      });
  }

  /**
   * broadcast savePresets event and also save preset to presetDict, which is a *view* (in the database sense) of DictionaryService's dictionary that only stores presets
   * @param type string, usually "operator"
   * @param target string, usualy operatorType
   * @param presets Preset[]
   * @param displayMessage message to display when saving presets
   * @param messageType see AlertMessageType, determines icon used in popup message
   */
  public updatePreset(
    type: string,
    target: string,
    originalPreset: Preset,
    replacementPreset: Preset,
    displayMessage?: string | null,
    messageType: AlertMessageType = "success"
  ) {
    this.userConfigService
      .fetchKey(`${type}-${target}`)
      .pipe(first())
      .subscribe(presetsString => {
        let presets = JSON.parse(presetsString ?? "[]") as Preset[];
        if (!contains(presets, originalPreset)) {
          throw new Error("attempting to update preset that doesn't exist");
        } else if (contains(presets, replacementPreset)) {
          // implicit deletion by replacing original with existing preset
          presets.splice(indexOf(presets, originalPreset), 1);
        } else {
          presets[indexOf(presets, originalPreset)] = replacementPreset;
        }
        this.savePresets(type, target, presets, displayMessage, messageType);
      });
  }

  /**
   * broadcast savePresets event and also save preset to presetDict, which is a *view* (in the database sense) of DictionaryService's dictionary that only stores presets
   * @param type string, usually "operator"
   * @param target string, usualy operatorType
   * @param presets Preset[]
   * @param displayMessage message to display when saving presets
   * @param messageType see AlertMessageType, determines icon used in popup message
   */
  public updateOrCreatePreset(
    type: string,
    target: string,
    originalPreset: Preset,
    replacementPreset: Preset,
    displayMessage?: string | null,
    messageType: AlertMessageType = "success"
  ) {
    this.userConfigService
      .fetchKey(`${type}-${target}`)
      .pipe(first())
      .subscribe(oldpresets => {
        let presets = JSON.parse(oldpresets ?? "[]") as Preset[];
        if (isEqual(originalPreset, replacementPreset)) {
          // no modification: no update required
        } else if (!contains(presets, originalPreset) && !contains(presets, replacementPreset)) {
          presets.push(replacementPreset);
        } else if (!contains(presets, originalPreset) && contains(presets, replacementPreset)) {
          // no modification: old preset doesn't exist to be updated, new preset already exists
        } else if (contains(presets, originalPreset) && contains(presets, replacementPreset)) {
          // implicit deletion by replacing original with existing preset
          presets.splice(indexOf(presets, originalPreset), 1);
        } else {
          presets[indexOf(presets, originalPreset)] = replacementPreset;
        }
        this.savePresets(type, target, presets, displayMessage, messageType);
      });
  }

  /**
   * broadcast savePresets event and also save preset to presetDict, which is a *view* (in the database sense) of DictionaryService's dictionary that only stores presets
   * removes preset if it exists
   * @param type string, usually "operator"
   * @param target string, usualy operatorType
   * @param preset preset to remove
   * @param displayMessage message to display when saving presets
   * @param messageType see AlertMessageType, determines icon used in popup message
   */
  public deletePreset(
    type: string,
    target: string,
    preset: Preset,
    displayMessage?: string | null,
    messageType: AlertMessageType = "error"
  ) {
    this.getPresets(type, target)
      .pipe(first())
      .subscribe(presets => {
        let modifiedPresets = presets.filter(oldPreset => !isEqual(oldPreset, preset));
        this.savePresets(type, target, modifiedPresets, displayMessage, messageType);
      });
  }

  /**
   * get presets from presetDict
   * @param type string, usually "operator"
   * @param target string, usualy operatorType
   * @returns Preset[]
   */
  public getPresets(type: string, target: string): Observable<Readonly<Preset[]>> {
    return this.userConfigService.fetchKey(`${type}-${target}`).pipe(
      map(presets => {
        let parsedPresets = JSON.parse(presets ?? "[]");
        if (this.isValidPresetArray(parsedPresets)) {
          return parsedPresets;
        } else {
          throw new Error(`stored preset data ${presets} is formatted incorrectly`);
        }
      })
    );
  }

  /**
   * extracts preset schema from operator schema and validates a preset with it
   * @param preset
   * @param operatorID
   * @returns boolean
   */
  public isValidOperatorPreset(preset: Preset, operatorID: string): boolean {
    const presetSchema = PresetService.getOperatorPresetSchema(
      this.operatorMetadataService.getOperatorSchema(
        this.workflowActionService.getTexeraGraph().getOperator(operatorID).operatorType
      ).jsonSchema
    );
    const fitsSchema = PresetService.ajv.compile(presetSchema)(preset);
    const noEmptyProperties = Object.keys(preset).every(
      (key: string) => !isType(preset[key], "string") || (<string>preset[key]).trim().length > 0
    );

    return fitsSchema && noEmptyProperties;
  }

  /**
   * extracts preset schema from operator schema and validates a preset with it.
   * also checks if preset exists in presetDict already.
   * @param preset
   * @param operatorID
   * @returns boolean
   */
  public isValidNewOperatorPreset(preset: Preset, operatorID: string): Observable<boolean> {
    if (!this.isValidOperatorPreset(preset, operatorID)) return of(false);

    return this.getPresets(
      "operator",
      this.workflowActionService.getTexeraGraph().getOperator(operatorID).operatorType
    ).pipe(
      first(),
      map(presets => {
        console.log(!presets.some(existingPreset => isEqual(preset, existingPreset)), "vn");
        return !presets.some(existingPreset => isEqual(preset, existingPreset));
      })
    );
  }

  public isValidPreset(preset: any): preset is Preset {
    return asType(PresetService.isPreset(preset), "boolean");
  }

  public isValidPresetArray(presets: any[]): presets is Preset[] {
    return asType(PresetService.isPresetArray(presets), "boolean");
  }

  private displaySavePresetMessage(messageType: AlertMessageType, displayMessage?: string | null) {
    if (displayMessage === null) return; // do not display explicitly null message
    if (displayMessage === undefined) {
      // if undefined, display default messages
      switch (messageType) {
        case "error":
          this.messageService.error("Preset deleted");
          break;
        case "info":
          throw new Error("no default save preset info message");
        // break;
        case "success":
          this.messageService.success("Preset saved");
          break;
        case "warning":
          throw new Error("no default save preset warning message");
        // break;
      }
    } else {
      // display explicitly passed message and messageType
      switch (messageType) {
        case "error":
          this.messageService.error(displayMessage);
          break;
        case "info":
          this.messageService.info(displayMessage);
          break;
        case "success":
          this.messageService.success(displayMessage);
          break;
        case "warning":
          this.messageService.warning(displayMessage);
          break;
      }
    }
  }

  /**
   * when presets are applied, check for operator presets, and apply them using workflowActionService
   * to change operator properties
   */
  private handleApplyOperatorPresets() {
    this.applyPresetStream.subscribe({
      next: applyEvent => {
        if (
          applyEvent.type === "operator" &&
          this.workflowActionService.getTexeraGraph().hasOperator(applyEvent.target)
        ) {
          if (this.isValidOperatorPreset(applyEvent.preset, applyEvent.target)) {
            this.workflowActionService.setOperatorProperty(
              applyEvent.target,
              merge(
                cloneDeep(
                  this.workflowActionService.getTexeraGraph().getOperator(applyEvent.target).operatorProperties
                ),
                applyEvent.preset
              )
            );
          } else {
            const schema = PresetService.getOperatorPresetSchema(
              this.operatorMetadataService.getOperatorSchema(
                this.workflowActionService.getTexeraGraph().getOperator(applyEvent.target).operatorType
              ).jsonSchema
            );
            throw new Error(
              `Error applying preset: preset ${applyEvent.preset} was not a valid preset for ${applyEvent.target} with schema ${schema}`
            );
          }
        }
      },
    });
  }

  /**
   * get preset schema from operator schema.
   * preset schema is just the operator schema with only properties that have 'enable-presets': true
   * all properties are required
   * @param operatorSchema
   * @returns preset schema
   */
  public static getOperatorPresetSchema(operatorSchema: CustomJSONSchema7): CustomJSONSchema7 {
    const copy = cloneDeep(operatorSchema);
    if (operatorSchema.properties === undefined)
      throw new Error(`provided operator schema ${operatorSchema} has no properties`);
    const properties = pickBy(
      copy.properties,
      prop => has(prop, "enable-presets") && (prop as any)["enable-presets"] === true
    );
    if (isEqual(properties, {})) throw new Error(`provided operator schema ${operatorSchema} has no preset properties`);
    return {
      type: "object",
      properties: properties,
      required: Object.keys(properties),
      additionalProperties: false,
    };
  }

  /**
   * get preset from operator properties if it has a preset schema and a valid preset (all properties are assigned)
   * Throws an error if operatorProperties doesn't have all the properties in the presetSchema, unlike filterOperatorProperties.
   * @param operatorSchema
   * @param operatorProperties
   * @returns Preset
   */
  public static getOperatorPreset(operatorSchema: CustomJSONSchema7, operatorProperties: object): Preset {
    const copy = cloneDeep(operatorProperties as Preset);
    const presetSchema = this.getOperatorPresetSchema(operatorSchema);
    const strip = this.ajvStrip.compile(presetSchema); // this validator also removes extra properties that aren't a part of the preset
    const result = strip(copy);
    if (asType(result, "boolean") === true) return copy;
    throw new Error(
      `provided operator properties ${operatorProperties} does not conform to preset schema ${presetSchema}`
    );
  }

  /**
   * get the subset of operatorProperties that only includes properties that are in its PresetSchema
   * this doesn't always yield a complete preset, unlike getOperatorPreset
   * @param operatorSchema
   * @param operatorProperties
   * @returns
   */
  public static filterOperatorPresetProperties(operatorSchema: CustomJSONSchema7, operatorProperties: object): Preset {
    const copy = cloneDeep(operatorProperties as Preset);
    const presetSchema = this.getOperatorPresetSchema(operatorSchema);
    const strip = this.ajvStrip.compile(presetSchema); // this validator also removes extra properties that aren't a part of the preset
    strip(copy);
    return copy;
  }
}

function contains(arr: any[], value: any) {
  return arr.some(elem => isEqual(elem, value));
}

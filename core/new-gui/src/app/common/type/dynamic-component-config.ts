import { Type } from "@angular/core";

export interface DynamicComponentConfig<T> {
  component?: Type<T>;
  componentInputs?: Partial<T>;
}

import { Type } from '@angular/core';

export declare class DynamicComponentConfig<T> {
  component?: Type<T>;
  componentInputs?: Partial<T>;
}

export type PublicInterfaceOf<Class> = {
  [Member in keyof Class]: Class[Member];
};

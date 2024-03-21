export function intersection<T>(setA: ReadonlySet<T>, setB: ReadonlySet<T>): Set<T> {
  let _intersection = new Set<T>();
  for (let elem of setA) {
    if (setB.has(elem)) {
      _intersection.add(elem);
    }
  }
  return _intersection;
}

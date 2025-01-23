function parseTranslate3d(translate3d: string): [number, number, number] {
  const regex = /translate3d\((-?\d+\.?\d*)px,\s*(-?\d+\.?\d*)px,\s*(-?\d+\.?\d*)px\)/g;
  const match = regex.exec(translate3d);
  if (match) {
    const x = parseFloat(match[1]);
    const y = parseFloat(match[2]);
    const z = parseFloat(match[3]);
    return [x, y, z];
  }
  return [0, 0, 0];
}

export function calculateTotalTranslate3d(translates: string): [number, number, number] {
  let totalXOffset = 0;
  let totalYOffset = 0;
  let totalZOffset = 0;

  const translate3dArray = translates.match(/translate3d\(.*?\)/g) || [];

  for (const translate of translate3dArray) {
    const [x, y, z] = parseTranslate3d(translate);
    totalXOffset += x;
    totalYOffset += y;
    totalZOffset += z;
  }

  return [totalXOffset, totalYOffset, totalZOffset];
}

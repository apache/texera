
export const MAIN_CANVAS_LIMIT = {
  xMin: -1680,
  xMax: 3360,
  yMin: -540,
  yMax: 1920
};

// amount of space mini_map takes compared to main canvas
export const MINI_MAP_ZOOM_SCALE = 0.12;
export const MINI_MAP_SIZE = {
  width:  (MAIN_CANVAS_LIMIT.xMax - MAIN_CANVAS_LIMIT.xMin) * MINI_MAP_ZOOM_SCALE,
  height: (MAIN_CANVAS_LIMIT.yMax - MAIN_CANVAS_LIMIT.yMin) * MINI_MAP_ZOOM_SCALE
};

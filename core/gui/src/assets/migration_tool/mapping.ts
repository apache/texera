interface MappingContent {
  cell_to_operator: { [key: string]: any };
  operator_to_cell: { [key: string]: any };
}

const mapping: { [key: string]: MappingContent } = {
  default: {
    cell_to_operator: {},
    operator_to_cell: {}
  }
};

export default mapping;

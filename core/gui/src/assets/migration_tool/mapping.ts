let mapping = {
  cell_to_operator: {
    "b5cc7f69-9128-4876-8ee4-17a17a0e79e9": [],
    "fe23f9aa-6474-4ac9-8f08-73b132550029": ["CSVFileScan-operator-43e5d7f4-2d3d-498b-a97d-b4d242ee82cf"],
    "4b172d9c-b974-4f8c-9b30-ed75a35a1b29": [
      "Distinct-operator-d6a18641-3c29-4835-b8d2-60351b0b8882",
      "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
    ],
    "1177f9a8-a014-43ab-ac3d-71ddabdd6917": [
      "Aggregate-operator-ba7c3a06-09f8-4ad3-977d-69988789e671",
      "Aggregate-operator-cd2cbbbf-bad0-46ed-9e13-65ccd1e67c27",
      "Aggregate-operator-3abc30e7-9ba3-4a97-ac28-937ca950cccf",
    ],
    "b5eac814-3c6d-4a62-9530-44475d13fba7": ["BoxPlot-operator-9ffd3dce-29b4-4373-ab38-235487393d72"],
    "730aa5e5-2b34-4182-8c6b-518d52a03c24": ["PythonUDFV2-operator-1d891fad-a245-4e03-ba40-ad9ffbfa2b6b"],
    "91cc0423-c670-4982-94d5-522de07057df": ["Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c"],
    "5db4f799-29bc-43ba-8aba-142f0db829f9": ["SklearnRandomForest-operator-7844ee52-5d28-4421-a537-d267bb52efa8"],
    "7764cfa5-70a4-48bc-99c8-b70c3db743ae": ["SklearnSVM-operator-7adceacc-b3f9-4af6-8319-cd3c3f96d95d"],
    "6297d0a8-48dc-4c29-9cc2-e5d5ca517001": ["SklearnDecisionTree-operator-65ea9d8c-9057-4ebc-8a40-a8c01333f948"],
    "68f22db7-5cd5-4c12-ad4c-3e750d0101b9": ["SklearnLogisticRegression-operator-eca48ee5-d808-4c5d-bb3c-6614d0bfb548"],
  },
  operator_to_cell: {
    "CSVFileScan-operator-43e5d7f4-2d3d-498b-a97d-b4d242ee82cf": ["fe23f9aa-6474-4ac9-8f08-73b132550029"],
    "Distinct-operator-d6a18641-3c29-4835-b8d2-60351b0b8882": ["4b172d9c-b974-4f8c-9b30-ed75a35a1b29"],
    "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04": ["4b172d9c-b974-4f8c-9b30-ed75a35a1b29"],
    "Aggregate-operator-ba7c3a06-09f8-4ad3-977d-69988789e671": ["1177f9a8-a014-43ab-ac3d-71ddabdd6917"],
    "Aggregate-operator-cd2cbbbf-bad0-46ed-9e13-65ccd1e67c27": ["1177f9a8-a014-43ab-ac3d-71ddabdd6917"],
    "Aggregate-operator-3abc30e7-9ba3-4a97-ac28-937ca950cccf": ["1177f9a8-a014-43ab-ac3d-71ddabdd6917"],
    "BoxPlot-operator-9ffd3dce-29b4-4373-ab38-235487393d72": ["b5eac814-3c6d-4a62-9530-44475d13fba7"],
    "PythonUDFV2-operator-1d891fad-a245-4e03-ba40-ad9ffbfa2b6b": ["730aa5e5-2b34-4182-8c6b-518d52a03c24"],
    "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c": ["91cc0423-c670-4982-94d5-522de07057df"],
    "SklearnRandomForest-operator-7844ee52-5d28-4421-a537-d267bb52efa8": ["5db4f799-29bc-43ba-8aba-142f0db829f9"],
    "SklearnSVM-operator-7adceacc-b3f9-4af6-8319-cd3c3f96d95d": ["7764cfa5-70a4-48bc-99c8-b70c3db743ae"],
    "SklearnDecisionTree-operator-65ea9d8c-9057-4ebc-8a40-a8c01333f948": ["6297d0a8-48dc-4c29-9cc2-e5d5ca517001"],
    "SklearnLogisticRegression-operator-eca48ee5-d808-4c5d-bb3c-6614d0bfb548": ["68f22db7-5cd5-4c12-ad4c-3e750d0101b9"],
  },
};

export default mapping;

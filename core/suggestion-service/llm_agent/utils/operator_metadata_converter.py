import json
import uuid
from typing import Dict, Any, List
import copy

# Load the operator metadata file
with open("files/operator_json_schema.json", "r") as f:
    operator_metadata = json.load(f)

valid_operator_types: set[str] = {
    schema["operatorType"] for schema in operator_metadata["operators"]
}


def resolve_ref(ref: str, definitions: Dict[str, Any]) -> Dict[str, Any]:
    try:
        ref_path = ref.lstrip("#/").split("/")
        resolved = definitions
        for part in ref_path:
            resolved = resolved.get(part, {})
        return resolved
    except Exception:
        return {}


def fill_defaults(
    schema: Dict[str, Any], definitions: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Fills default values into a schema-defined object.
    """
    if "$ref" in schema:
        schema = resolve_ref(schema["$ref"], definitions)

    if schema.get("type") == "object":
        obj = {}
        for prop, subschema in schema.get("properties", {}).items():
            obj[prop] = fill_defaults(subschema, definitions)
        return obj
    elif schema.get("type") == "array":
        return []
    elif "default" in schema:
        return copy.deepcopy(schema["default"])
    return None


def convert_to_operator_predicate(schema: Dict[str, Any]) -> Dict[str, Any]:
    operator_type = schema["operatorType"]
    metadata = schema["additionalMetadata"]
    json_schema = schema["jsonSchema"]
    definitions = json_schema.get("definitions", {})

    operator_id = f"{operator_type}-operator-{uuid.uuid4()}"
    operator_properties = fill_defaults(json_schema, definitions)

    input_ports = []
    for i, port_info in enumerate(metadata.get("inputPorts", [])):
        input_ports.append(
            {
                "portID": f"input-{i}",
                "displayName": port_info.get("displayName", ""),
                "allowMultiInputs": port_info.get("allowMultiLinks", False),
                "isDynamicPort": False,
                "dependencies": port_info.get("dependencies", []),
            }
        )

    output_ports = []
    for i, port_info in enumerate(metadata.get("outputPorts", [])):
        output_ports.append(
            {
                "portID": f"output-{i}",
                "displayName": port_info.get("displayName", ""),
                "allowMultiInputs": False,
                "isDynamicPort": False,
            }
        )

    return {
        "operatorID": operator_id,
        "operatorType": operator_type,
        "operatorVersion": schema.get("operatorVersion", "N/A"),
        "operatorProperties": operator_properties,
        "inputPorts": input_ports,
        "outputPorts": output_ports,
        "dynamicInputPorts": metadata.get("dynamicInputPorts", False),
        "dynamicOutputPorts": metadata.get("dynamicOutputPorts", False),
        "showAdvanced": False,
        "isDisabled": False,
        "customDisplayName": metadata.get("userFriendlyName", operator_type),
    }


def _extract_json_schema(
    operator_type: str, properties_only: bool = True
) -> Dict[str, Any]:
    """
    Extract the full or properties-only JSON schema of an operatorType.

    Args:
        operator_type: The operatorType string to search for
        properties_only: If True, only include "properties", "definitions", "required" fields
                         and remove dummy/partition-related parts

    Returns:
        A dictionary containing the extracted JSON schema.
    """
    if operator_type not in valid_operator_types:
        raise ValueError(f"OperatorType '{operator_type}' not found in metadata.")

    for operator_schema in operator_metadata["operators"]:
        if operator_schema["operatorType"] == operator_type:
            json_schema = operator_schema.get("jsonSchema")
            if not json_schema:
                raise ValueError(
                    f"No jsonSchema found for operatorType '{operator_type}'"
                )

            if not properties_only:
                return {"operatorType": operator_type, "jsonSchema": json_schema}

            # Deepcopy to avoid modifying the original schema
            filtered_schema = {
                "properties": {},
                "definitions": {},
                "required": json_schema.get("required", []),
            }

            for prop_name, prop_value in json_schema.get("properties", {}).items():
                if prop_name != "dummyPropertyList":
                    filtered_schema["properties"][prop_name] = prop_value

            for def_name, def_value in json_schema.get("definitions", {}).items():
                if def_name != "DummyProperties" and not def_name.endswith("Partition"):
                    filtered_schema["definitions"][def_name] = def_value

            return {"operatorType": operator_type, "jsonSchema": filtered_schema}

    raise ValueError(f"OperatorType '{operator_type}' not found unexpectedly.")


if __name__ == "__main__":
    # Convert all schemas
    operator_predicates: List[Dict[str, Any]] = [
        convert_to_operator_predicate(schema)
        for schema in operator_metadata["operators"]
    ]
    output_path = "files/operator_format.json"
    with open(output_path, "w") as f:
        json.dump(operator_predicates, f, indent=2)

    operator_type_filepath = "files/operator_type.txt"
    with open(operator_type_filepath, "w") as f:
        f.write(str(list(valid_operator_types)))

    md_path = "files/operator_type_and_descriptions.md"
    with open(md_path, "w") as f:
        # optional title – remove if you don't want a header
        f.write("# Operator Types and Descriptions\n\n")

        for op in operator_metadata["operators"]:
            op_type = op["operatorType"]
            op_desc = op.get("additionalMetadata", {}).get("operatorDescription", "")
            # fallback if description is missing
            if not op_desc:
                op_desc = "(no description provided)"
            f.write(f"- {op_type}: {op_desc}\n")


def extract_json_schemas(operator_types: List[str]) -> List[Dict[str, Any]]:
    return [_extract_json_schema(op_type, True) for op_type in operator_types]

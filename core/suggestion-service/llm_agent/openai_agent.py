import json
import uuid
from typing import Dict, Any, List, Optional

from openai import OpenAI
from llm_agent.base import LLMAgent, LLMAgentFactory
from llm_agent.utils.operator_metadata_converter import extract_json_schemas
from model.llm import SuggestionPrompt
from model.llm.interpretation import BaseInterpretation
from model.llm.prompt import DataCleaningSuggestionPrompt
from model.llm.suggestion import SuggestionList, Suggestion, DataCleaningSuggestionList


@LLMAgentFactory.register("openai")
class OpenAIAgent(LLMAgent):
    """
    Implementation of the LLM agent using OpenAI's `responses.create` API
    to both generate and sanitize workflow suggestions.
    """

    def __init__(
        self,
        model,
        tools,
        api_key,
        project,
        organization,
        use_function_calls: bool = True,
    ):
        """
        Initialize the OpenAI agent.

        Args:
            model: OpenAI model name (e.g., "gpt-4o-2024-08-06")
            api_key: API key for OpenAI
            project: Project ID for usage (optional)
        """
        super().__init__(model, api_key)
        self.model = model
        self.tools = tools
        self.client = OpenAI(
            api_key=api_key, project=project, organization=organization
        )

        # Load first phase instruction
        with open("files/instruction_for_suggestion.md", "r") as f:
            self.instruction_for_suggestion = f.read()

        # Load sanitizer instruction
        with open("files/instruction_for_sanitizor.md", "r") as f:
            self.instruction_for_sanitizor = f.read()

        with open("files/instruction_for_function_call.md", "r") as f:
            self.instruction_for_suggestion_fc = f.read()

        with open("files/instruction_for_data_cleaning_agent.md", "r") as f:
            self.instruction_for_dc_suggestion = f.read()

        self.function_tools = [
            {
                "type": "function",
                "name": "extract_json_schemas",
                "description": (
                    "Return a list of JSON schemas for given Texera operator types. "
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operator_types": {
                            "type": "array",
                            "description": "List of operatorType names to fetch.",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["operator_types"],
                    "additionalProperties": False,
                },
                "strict": True,
            }
        ]

        self.use_function_calls = use_function_calls
        self.enable_llm_sanitizor = False

    def generate_suggestions(
        self,
        prompt: SuggestionPrompt,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> SuggestionList:
        if not self.use_function_calls:
            # fall back to the old single‑shot method
            return self._generate_suggestions_one_shot(prompt, temperature, max_tokens)
        else:
            # NEW two‑turn method
            return self._generate_suggestions_with_function_calls(
                prompt, temperature, max_tokens
            )

    def _generate_suggestions_one_shot(self, prompt, temperature, max_tokens):
        """
        Generate workflow suggestions using OpenAI's `responses.create` endpoint with schema enforcement.

        Args:
            prompt: Workflow description
            temperature: Sampling temperature
            max_tokens: Maximum tokens allowed in output

        Returns:
            A list of sanitized suggestion dicts.
        """
        try:
            prompt_json: str = prompt.model_dump_json(indent=2)
            # Step 1: Generate raw suggestions
            raw_response = self.client.responses.create(
                model=self.model,
                instructions=self.instruction_for_suggestion,
                input=prompt_json,
                tools=self.tools,
                # text_format=SuggestionList
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "workflow_suggestions",
                        "schema": SuggestionList.model_json_schema(),
                        "strict": False,  # Allow flexible output in generation
                    }
                },
            )

            raw_suggestions = json.loads(raw_response.output_text)["suggestions"]
            with open("raw_suggestions_debug.json", "w") as f:
                json.dump(raw_suggestions, f, indent=2)
            # Step 2: Sanitize the suggestions
            sanitized_suggestions = self._sanitize_suggestions(
                raw_suggestions, prompt.workflowInterpretation
            )

            return sanitized_suggestions

        except Exception as e:
            print(f"Error generating suggestions: {e}")
            return SuggestionList(suggestions=[])

    def _id_set_from_workflow(self, workflow_json: Dict[str, Any]):
        """
        Return the sets of operator IDs and link IDs that already exist
        in the workflow (after .model_dump()).

        * operators  – dict mapping id → details
        * links      – list of link objects, each MAY contain linkID
        """
        # Operators: keys of the dict
        ops = set(workflow_json.get("operators", {}).keys())

        # Links: collect linkID if present (model_dump() may omit it)
        links = set()
        for link in workflow_json.get("links", []):
            if isinstance(link, dict):
                lid = link.get("linkID")
                if lid:  # skip if missing / None
                    links.add(lid)

        return ops, links

    def _valid_suggestion_structure(self, s: Dict[str, Any]) -> bool:
        """Quick structural check before deeper validation."""
        return (
            isinstance(s.get("suggestion"), str)
            and s.get("suggestionType") in {"fix", "improve"}
            and isinstance(s.get("changes"), dict)
            and all(
                k in s["changes"]
                for k in (
                    "operatorsToAdd",
                    "linksToAdd",
                    "operatorsToDelete",
                    "linksToDelete",
                )
            )
        )

    def _sanitize_suggestions(
        self,
        suggestions: List[Dict[str, Any]],
        workflow_intp: BaseInterpretation,
    ) -> SuggestionList:
        """
        Validate / repair suggestions according to the project rules.
        Invalid suggestions are dropped.  In addition, port IDs in
        `linksToAdd` are normalised to the Texera conventions:
            •  sources  →  "output-<index>"   (default index = 0)
            •  targets  →  "input-<index>"    (default index = 0)
        """

        # ---------- helper --------------------------------------------------
        def _normalise_port_id(raw_pid: Any, direction: str) -> str:
            """
            Coerce various user/LLM styles into the canonical form.

            direction ∈ {"input", "output"}
            """
            if raw_pid is None:
                return f"{direction}-0"

            pid = str(raw_pid).strip().lower()

            # already canonical
            if pid.startswith(f"{direction}-"):
                return pid

            # just a bare integer (e.g. "0")
            if pid.isdigit():
                return f"{direction}-{pid}"

            # naked "input"/"output" → assume 0
            if pid in {"input", "output"}:
                return f"{direction}-0"

            # anything else – fall back to 0
            return f"{direction}-0"

        # --------------------------------------------------------------------

        try:
            workflow_dict = (
                workflow_intp.get_base_workflow_interpretation().model_dump()
            )

            existing_ops, existing_links = self._id_set_from_workflow(workflow_dict)
            cleaned: List[Suggestion] = []
            operator_types_cache: Dict[str, Any] = {}

            for raw in suggestions:
                # 0) basic shape & enums -------------------------------------------------
                if not self._valid_suggestion_structure(raw):
                    continue

                ch = raw["changes"]

                # 1) operatorsToAdd / updates ------------------------------------------
                for op in ch["operatorsToAdd"]:
                    # schema check (and cache)
                    optype = op["operatorType"]
                    if optype not in operator_types_cache:
                        try:
                            operator_types_cache[optype] = extract_json_schemas(
                                [optype]
                            )[0]
                        except ValueError:
                            break  # invalid operator type -> drop suggestion

                    # if updating an existing op, ID must exist in workflow
                    # (otherwise it's a brand-new op, which is fine)
                else:
                    # 2) linksToAdd ------------------------------------------------------
                    valid_link_add = True
                    for link in ch["linksToAdd"]:
                        # ---------- NEW: fix / fill port IDs --------------------------
                        # NB: we *mutate* the dict in-place so later checks see the
                        #     corrected value.
                        src_port = link.setdefault("source", {})
                        tgt_port = link.setdefault("target", {})
                        src_port["portID"] = _normalise_port_id(
                            src_port.get("portID"), direction="output"
                        )
                        tgt_port["portID"] = _normalise_port_id(
                            tgt_port.get("portID"), direction="input"
                        )
                        # --------------------------------------------------------------

                        # ensure linkID exists (generate if missing)
                        if not link.get("linkID"):
                            link["linkID"] = f"link-{uuid.uuid4()}"

                        # ensure endpoints refer to real or newly-added ops
                        src_ok = src_port["operatorID"] in existing_ops or any(
                            op["operatorID"] == src_port["operatorID"]
                            for op in ch["operatorsToAdd"]
                        )
                        tgt_ok = tgt_port["operatorID"] in existing_ops or any(
                            op["operatorID"] == tgt_port["operatorID"]
                            for op in ch["operatorsToAdd"]
                        )
                        if not (src_ok and tgt_ok):
                            valid_link_add = False
                            break
                    if not valid_link_add:
                        continue  # invalid suggestion

                    # 3) deletions ------------------------------------------------------
                    if not set(ch["operatorsToDelete"]).issubset(existing_ops):
                        continue
                    if not set(ch["linksToDelete"]).issubset(existing_links):
                        continue

                    # 4) final parse to pydantic ---------------------------------------
                    try:
                        cleaned.append(Suggestion(**raw))
                    except Exception:
                        continue  # any remaining schema errors – drop

            return SuggestionList(suggestions=cleaned)

        except Exception as e:
            print(f"Error sanitizing suggestions: {e}")
            return SuggestionList(suggestions=[])

        except Exception as e:
            print(f"Error sanitizing suggestions: {e}")
            return SuggestionList(suggestions=[])

    def _call_function(self, name, args):
        if name == "extract_json_schemas":
            return extract_json_schemas(**args)
        else:
            raise ValueError(f"Unknown function: {name}")

    def _generate_suggestions_with_function_calls(
        self, prompt: SuggestionPrompt, temperature: float, max_tokens: Optional[int]
    ) -> SuggestionList:
        """
        Two‑step approach:
          1) Ask the model which operator types it needs & have it call
             `extract_json_schemas` (one call per distinct type).
          2) Execute those calls locally, hand results back, let the model
             produce the final SuggestionList JSON.
        """
        prompt_json: str = prompt.model_dump_json(indent=2)
        input_messages = [{"role": "user", "content": prompt_json}]
        # -- 1️⃣  first turn: let the model issue tool calls ------------------
        first_resp = self.client.responses.create(
            model=self.model,
            instructions=self.instruction_for_suggestion_fc,
            input=input_messages,
            tool_choice="required",
            tools=self.function_tools,
        )

        for tool_call in first_resp.output:
            if tool_call.type != "function_call":
                continue

            name = tool_call.name
            args = json.loads(tool_call.arguments)

            result = self._call_function(name, args)
            input_messages.append(
                {
                    "type": "function_call",
                    "call_id": tool_call.call_id,
                    "name": tool_call.name,
                    "arguments": tool_call.arguments,
                }
            )
            input_messages.append(
                {
                    "type": "function_call_output",
                    "call_id": tool_call.call_id,
                    "output": str(result),
                }
            )

        # -- 3️⃣  second turn: give the model the schemas & ask for suggestions
        second_resp = self.client.responses.create(
            model=self.model,
            input=input_messages,
            instructions=self.instruction_for_suggestion_fc,
            tools=self.function_tools,
            tool_choice="none",
            text={
                "format": {  # enforce SuggestionList schema again
                    "type": "json_schema",
                    "name": "workflow_suggestions",
                    "schema": SuggestionList.model_json_schema(),
                    "strict": False,
                }
            },
        )
        raw_suggestions = json.loads(second_resp.output_text)["suggestions"]
        with open("raw_suggestions_debug.json", "w") as f:
            json.dump(raw_suggestions, f, indent=2)

        sanitized_suggestions = self._sanitize_suggestions(
            raw_suggestions, prompt.workflowInterpretation
        )

        return sanitized_suggestions

    def generate_data_cleaning_suggestions(
        self,
        prompt: DataCleaningSuggestionPrompt,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> DataCleaningSuggestionList:
        """
        Produce a short list of natural-language cleaning recommendations
        for the given column profile.

        The model is instructed to return a JSON object that matches
        `DataCleaningSuggestionList` (see model.llm.suggestion).
        """

        try:
            prompt_json = prompt.model_dump_json(indent=2)

            resp = self.client.responses.create(
                model=self.model,
                instructions=self.instruction_for_dc_suggestion,
                input=prompt_json,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "data_cleaning_suggestions",
                        "schema": DataCleaningSuggestionList.model_json_schema(),
                        "strict": True,  # let the model vary order / whitespace
                        "additionalProperties": False,
                    }
                },
            )

            # The assistant output is guaranteed JSON thanks to the schema-
            # enforcement, so we can safely load then validate with pydantic.
            suggestions_dict = json.loads(resp.output_text)
            return suggestions_dict

        except Exception as e:
            # Any error → return an empty list to the caller
            print(f"[OpenAIAgent] Error in data-cleaning generation: {e}")
            return DataCleaningSuggestionList(suggestions=[])

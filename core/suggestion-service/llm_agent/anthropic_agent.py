"""Anthropic Claude LLM agent."""

import os
import json
from typing import Dict, Any, List, Optional

import anthropic
from anthropic import Anthropic

from llm_agent.base import LLMAgent, LLMAgentFactory
from model.llm.suggestion import SuggestionList


@LLMAgentFactory.register("anthropic")
class AnthropicAgent(LLMAgent):
    """
    Implementation of the LLM agent interface using Anthropic's Claude API.

    This agent supports different Claude models, such as:
    - claude-3-opus-20240229
    - claude-3-sonnet-20240229
    - claude-3-haiku-20240307
    - claude-2.1
    - claude-2.0
    """

    def __init__(
        self, model: str = "claude-3-opus-20240229", api_key: Optional[str] = None
    ):
        """
        Initialize the Anthropic Claude agent.

        Args:
            model: The Claude model to use
            api_key: The Anthropic API key (if None, uses ANTHROPIC_API_KEY environment variable)
        """
        self.model = model
        self.client = Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"))

    def generate_suggestions(
        self,
        prompt: str,
        max_suggestions: int = 3,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> SuggestionList:
        """
        Generate workflow suggestions using Anthropic's Claude API.

        Args:
            prompt: The natural language prompt describing the workflow
            max_suggestions: Maximum number of suggestions to generate
            temperature: Sampling temperature (0.0-1.0) where lower is more deterministic
            max_tokens: Maximum number of tokens to generate (if None, defaults to 4096)
            **kwargs: Additional Claude-specific parameters

        Returns:
            A list of suggestion dictionaries formatted according to the interface
        """
        # Enhance the prompt with instruction about the output format
        system_prompt = self._create_system_prompt(max_suggestions)

        try:
            response = self.client.messages.create(
                model=self.model,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens or 4096,
                **kwargs,
            )

            # Extract content from the response
            raw_content = response.content[0].text

            return raw_content[:max_suggestions]

        except Exception as e:
            print(f"Error generating suggestions with Anthropic Claude: {str(e)}")
            return []

    def _create_system_prompt(self, max_suggestions: int) -> str:
        """Create the system prompt for the Claude model."""
        return f"""You are an AI assistant that helps users improve their Texera workflows by suggesting useful modifications.

Analyze the provided workflow description and generate {max_suggestions} suggestions to improve it.
Your suggestions should address common issues, optimizations, or additional useful features.

For each suggestion, provide:
1. A brief natural language description of the recommendation
2. Detailed changes required to implement the suggestion

Structure each suggestion as a valid JSON object with the following format:
{{
    "suggestion": "A clear description of the improvement",
    "changes": {{
        "operatorsToAdd": [
            {{
                "operatorType": "TypeOfOperator",
                "operatorID": "OperatorType-operator-UUID",
                "operatorProperties": {{
                    "property1": "value1",
                    "property2": "value2"
                }},
                "customDisplayName": "Logical name describing function"
            }}
        ],
        "linksToAdd": [
            {{
                "linkID": "link-UUID",
                "source": {{
                    "operatorID": "SourceOperatorID",
                    "portID": "output-N"
                }},
                "target": {{
                    "operatorID": "TargetOperatorID",
                    "portID": "input-N"
                }}
            }}
        ],
        "operatorsToDelete": ["OperatorID1", "OperatorID2"],
        "operatorPropertiesToChange": [
            {{
                "operatorID": "ExistingOperatorID",
                "properties": {{
                    "propertyToChange": "newValue"
                }}
            }}
        ]
    }}
}}

Output all suggestions as a valid JSON array and ensure each suggestion follows the exact format above.
Be sure to only generate valid JSON in your response."""

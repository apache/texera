"""Base LLM agent interface"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Type
import os

from model.llm import SuggestionPrompt
from model.llm.prompt import DataCleaningSuggestionPrompt
from model.llm.suggestion import SuggestionList, DataCleaningSuggestionList


class LLMAgent(ABC):
    """
    Abstract base class for LLM agents that generate workflow suggestions.
    All LLM providers (OpenAI, Anthropic, etc.) should implement this interface.
    """

    def __init__(self, model: str = None, api_key: str = None):
        """
        Initialize the LLM agent.

        Args:
            model: The model to use for generation
            api_key: The API key for the LLM service
        """
        self.model = model
        self.api_key = api_key

    @abstractmethod
    def generate_suggestions(
        self,
        prompt: SuggestionPrompt,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> SuggestionList:
        """
        Generate workflow suggestions using the LLM provider.

        Args:
            prompt: The natural language prompt describing the workflow
            max_suggestions: Maximum number of suggestions to generate
            temperature: Sampling temperature (0.0-1.0) where lower is more deterministic
            max_tokens: Maximum number of tokens to generate
            **kwargs: Additional provider-specific parameters

        Returns:
            A list of suggestion dictionaries with the following format:
            {
                "suggestion": str,                  # Natural language description of the suggestion
                "changes": {
                    "operatorsToAdd": List[Dict],   # Operators to add to the workflow
                    "linksToAdd": List[Dict],       # Links to add between operators
                    "operatorsToDelete": List[str], # Operator IDs to delete
                    "operatorPropertiesToChange": List[Dict], # Properties to change on existing operators
                }
            }
        """
        pass

    @abstractmethod
    def generate_data_cleaning_suggestions(
        self,
        prompt: DataCleaningSuggestionPrompt,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> DataCleaningSuggestionList:
        """
        generate the natural language plan of cleaning the given column
        """
        pass


class LLMAgentFactory:
    """Factory for creating LLM agents based on provider name."""

    _agent_registry: Dict[str, Type[LLMAgent]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Decorator to register an LLM agent class with the factory.

        Args:
            name: The name to register the agent under

        Returns:
            A decorator function
        """

        def decorator(agent_class):
            cls._agent_registry[name.lower()] = agent_class
            return agent_class

        return decorator

    @classmethod
    def create(cls, provider: str, model: str, **kwargs) -> LLMAgent:
        """
        Create an LLM agent instance based on the provider.

        Args:
            provider: The LLM provider to use (e.g., 'openai', 'anthropic')
            model: The model to use (if None, will use default for the provider)
            api_key: The API key (if None, will use environment variable)
            **kwargs: Additional parameters to pass to the agent constructor

        Returns:
            An instance of the appropriate LLM agent

        Raises:
            ValueError: If the provider is not registered or if required configuration is missing
        """
        provider = provider.lower()

        if provider not in cls._agent_registry:
            raise ValueError(
                f"LLM provider '{provider}' is not supported. Available providers: {list(cls._agent_registry.keys())}"
            )

        # Get the agent class from the registry
        agent_class = cls._agent_registry[provider]

        # Determine API key from environment if not provided
        if provider == "openai":
            api_key = os.environ.get("OPENAI_API_KEY")
        elif provider == "anthropic":
            api_key = os.environ.get("ANTHROPIC_API_KEY")

        # Check if API key is available
        if not api_key:
            raise ValueError(
                f"API key for {provider} not provided and not found in environment variables"
            )

        # Create and return the agent instance
        return agent_class(model=model, api_key=api_key, **kwargs)

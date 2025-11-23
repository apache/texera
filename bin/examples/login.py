"""
Authentication module for Texera API.
Handles user login and token management.
"""

import requests
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class TexeraAuth:
    """Handle authentication with Texera server."""

    def __init__(self, base_url: str):
        """
        Initialize authentication handler.

        Args:
            base_url: Base URL of Texera server (e.g., http://localhost:8080)
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api"
        self.token: Optional[str] = None

    def login(self, username: str, password: str) -> str:
        """
        Login to Texera and obtain access token.

        Args:
            username: Username for authentication
            password: Password for authentication

        Returns:
            Access token (JWT)

        Raises:
            Exception: If login fails
        """
        url = f"{self.api_url}/auth/login"
        payload = {
            "username": username,
            "password": password
        }

        logger.info(f"Logging in to {url} as {username}")

        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()

            data = response.json()
            self.token = data.get("accessToken")

            if not self.token:
                raise Exception("No access token received from server")

            logger.info("Login successful")
            return self.token

        except requests.exceptions.RequestException as e:
            logger.error(f"Login failed: {e}")
            raise Exception(f"Failed to login: {e}")

    def register(self, username: str, password: str) -> str:
        """
        Register a new user and obtain access token.

        Args:
            username: Username for new account
            password: Password for new account

        Returns:
            Access token (JWT)

        Raises:
            Exception: If registration fails
        """
        url = f"{self.api_url}/auth/register"
        payload = {
            "username": username,
            "password": password
        }

        logger.info(f"Registering new user {username}")

        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()

            data = response.json()
            self.token = data.get("accessToken")

            if not self.token:
                raise Exception("No access token received from server")

            logger.info("Registration successful")
            return self.token

        except requests.exceptions.RequestException as e:
            logger.error(f"Registration failed: {e}")
            raise Exception(f"Failed to register: {e}")

    def get_headers(self) -> dict:
        """
        Get authorization headers for API requests.

        Returns:
            Dictionary with Authorization header

        Raises:
            Exception: If not authenticated
        """
        if not self.token:
            raise Exception("Not authenticated. Please login first.")

        return {
            "Authorization": f"Bearer {self.token}"
        }

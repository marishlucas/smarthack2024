# src/api_client.py

import requests
from typing import Optional, Dict, Any, List
import logging
import json


class APIClient:
    """Client for interacting with the fuel optimization API"""

    def __init__(self, api_key: str, base_url: str = "http://localhost:8080"):
        self.api_key = api_key
        self.base_url = base_url
        self.session_id: "eacffab2-0e36-4309-9abb-415297a6b406"
        self.headers = {"API-KEY": self.api_key, "Content-Type": "application/json"}
        self.max_retries = 3
        self.retry_delay = 1  # seconds

    def _parse_response(
        self, response: requests.Response, expect_json: bool = True
    ) -> Dict[str, Any]:
        """
        Parse API response

        Args:
            response: Response object from requests
            expect_json: Whether to expect JSON response

        Returns:
            Parsed response data
        """
        try:
            text = response.text.strip()
            if not text:
                return {}

            if expect_json:
                return json.loads(text)
            else:
                # Handle plain text response
                return {"sessionId": text}

        except json.JSONDecodeError as e:
            if not expect_json:
                # If we're not expecting JSON and parsing failed, return the text as is
                return {"sessionId": text}
            logging.error(f"Failed to parse JSON response: {response.text}")
            logging.error(f"JSON decode error: {str(e)}")
            return {}

    def start_session(self) -> bool:
        """Start a new session"""
        try:
            response = requests.post(
                f"{self.base_url}/api/v1/session/start", headers=self.headers
            )

            logging.debug(f"Start session response status: {response.status_code}")
            logging.debug(f"Start session response headers: {response.headers}")
            logging.debug(f"Start session response body: {response.text}")

            if response.status_code == 200:
                # Parse response as plain text since we know it's just a session ID
                session_data = self._parse_response(response, expect_json=False)

                self.session_id = session_data.get("sessionId")

                if not self.session_id:
                    logging.error("Session ID not found in response")
                    return False

                self.headers["SESSION-ID"] = self.session_id
                logging.info(
                    f"Session started successfully. Session ID: {self.session_id}"
                )
                return True

            elif response.status_code == 409:
                logging.warning("An active session exists. Attempting to end it.")
                if self.end_session():
                    return self.start_session()
                return False
            else:
                logging.error(
                    f"Failed to start session: {response.status_code} - {response.text}"
                )
                return False

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed while starting session: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Error starting session: {str(e)}")
            return False

    def play_round(
        self, current_day: int, movements: List[Dict]
    ) -> Optional[Dict[str, Any]]:
        """Play a round with the given movements"""
        if not self.session_id:
            logging.error("No active session. Please start a session first.")
            return None

        payload = {
            "day": current_day,
            "movements": [
                {"connectionId": m["connectionId"], "amount": m["amount"]}
                for m in movements
            ],
        }

        try:
            response = requests.post(
                f"{self.base_url}/api/v1/play/round", headers=self.headers, json=payload
            )

            logging.debug(f"Play round response status: {response.status_code}")
            logging.debug(f"Play round response headers: {response.headers}")
            logging.debug(f"Play round response body: {response.text}")

            if response.status_code == 200:
                return self._parse_response(response, expect_json=True)
            else:
                logging.error(
                    f"Failed to play round: {response.status_code} - {response.text}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed while playing round: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Error playing round: {str(e)}")
            return None

    def end_session(self) -> bool:
        """End the current session"""
        if not self.session_id:
            logging.warning("No active session to end.")
            return False

        try:
            response = requests.post(
                f"{self.base_url}/api/v1/session/end", headers=self.headers
            )

            logging.debug(f"End session response status: {response.status_code}")
            logging.debug(f"End session response headers: {response.headers}")
            logging.debug(f"End session response body: {response.text}")

            if response.status_code == 200:
                logging.info("Session ended successfully")
                self.session_id = None
                self.headers.pop("SESSION-ID", None)
                return True
            else:
                logging.error(
                    f"Failed to end session: {response.status_code} - {response.text}"
                )
                return False

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed while ending session: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Error ending session: {str(e)}")
            return False

    def check_session_status(self) -> Optional[bool]:
        """Check if there is an active session"""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/session/status", headers=self.headers
            )

            logging.debug(f"Check status response status: {response.status_code}")
            logging.debug(f"Check status response headers: {response.headers}")
            logging.debug(f"Check status response body: {response.text}")

            if response.status_code == 200:
                status_data = self._parse_response(response, expect_json=True)
                return status_data.get("active", False)
            else:
                logging.error(
                    f"Failed to check session status: {response.status_code} - {response.text}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed while checking session status: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Error checking session status: {str(e)}")
            return None


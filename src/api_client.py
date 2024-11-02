import requests
from typing import Optional, Dict, Any, List
import logging
import json
import time


class APIClient:
    """Client for interacting with the fuel optimization API"""

    def __init__(self, api_key: str, base_url: str = "http://localhost:8080"):
        self.api_key = api_key
        self.base_url = base_url
        self.session_id: Optional[str] = None
        self.headers = {
            "API-KEY": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.max_retries = 3
        self.retry_delay = 1  # seconds

    def _parse_response(
        self, response: requests.Response, expect_json: bool = True
    ) -> Dict[str, Any]:
        """
        Parse API response

        Args:
            response: Response object from requests
            expect_json: Whether to expect JSON response (default True)
        """
        try:
            if not response.text:
                logging.warning("Empty response received")
                return {}

            if expect_json:
                return response.json()
            else:
                # For non-JSON responses, return as-is in a dict
                return {"response": response.text.strip()}

        except json.JSONDecodeError as e:
            if expect_json:
                logging.error(f"Failed to parse JSON response: {response.text}")
                logging.error(f"JSON decode error: {str(e)}")
                return {}
            else:
                # For non-JSON responses, return as-is in a dict
                return {"response": response.text.strip()}

    def start_session(self) -> bool:
        """Start a new session"""
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/api/v1/session/start",
                    headers=self.headers,
                    timeout=10,
                )

                logging.info(f"Start session response status: {response.status_code}")
                logging.debug(f"Start session response headers: {response.headers}")
                logging.debug(f"Start session response body: {response.text}")

                if response.status_code == 200:
                    # Parse response expecting non-JSON (plain text session ID)
                    session_data = self._parse_response(response, expect_json=False)

                    # Try to get session ID from plain text response
                    self.session_id = session_data.get("response")

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
                        time.sleep(self.retry_delay)  # Wait before retrying
                        continue
                    return False
                else:
                    logging.error(
                        f"Failed to start session: {response.status_code} - {response.text}"
                    )
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                    return False

            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed while starting session: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return False
            except Exception as e:
                logging.error(f"Error starting session: {str(e)}")
                return False

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

        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/api/v1/play/round",
                    headers=self.headers,
                    json=payload,
                    timeout=10,
                )

                logging.info(f"Play round response status: {response.status_code}")
                logging.debug(f"Play round response headers: {response.headers}")
                logging.debug(f"Play round response body: {response.text}")

                if response.status_code == 200:
                    return self._parse_response(response, expect_json=True)
                else:
                    logging.error(
                        f"Failed to play round: {response.status_code} - {response.text}"
                    )
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                    return None

            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed while playing round: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return None
            except Exception as e:
                logging.error(f"Error playing round: {str(e)}")
                return None

        return None

    def end_session(self) -> bool:
        """End the current session"""
        if not self.session_id:
            logging.warning("No active session to end.")
            return False

        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/api/v1/session/end",
                    headers=self.headers,
                    timeout=10,
                )

                logging.info(f"End session response status: {response.status_code}")
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
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                    return False

            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed while ending session: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                return False
            except Exception as e:
                logging.error(f"Error ending session: {str(e)}")
                return False

        return False


"""
This class replaces database_queries.py to act as a proxy between the POSSUM database and the code.
We can no longer directly query the database, so we use the REST API instead.
"""
import os
from threading import Lock

import requests
from dotenv import load_dotenv

load_dotenv()


class PossumApiClient:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            # ensure there is only 1 instance of client
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False

        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.base_url = os.environ["POSSUM_API_URL"].rstrip("/")
        self.username = os.environ["POSSUM_API_USERNAME"]
        self.password = os.environ["POSSUM_API_PASSWORD"]

        self.session = requests.Session()

        self.access_token = None
        self.refresh_token = None

        self._initialized = True

    def login(self):
        response = self.session.post(
            f"{self.base_url}/api/token/",
            json={
                "username": self.username,
                "password": self.password,
            },
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        self.access_token = data["access"]
        self.refresh_token = data["refresh"]

    def refresh_access_token(self):
        if not self.refresh_token:
            self.login()
            return

        response = self.session.post(
            f"{self.base_url}/api/token/refresh/",
            json={"refresh": self.refresh_token},
            timeout=30,
        )

        if response.status_code == 401:
            self.login()
            return

        response.raise_for_status()

        self.access_token = response.json()["access"]

    def _ensure_authenticated(self):
        if self.access_token is None:
            self.login()

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
        }

    def _request(self, method, endpoint, **kwargs):
        self._ensure_authenticated()

        response = self.session.request(
            method=method,
            url=f"{self.base_url}{endpoint}",
            headers=self._headers(),
            timeout=30,
            **kwargs,
        )

        if response.status_code == 401:
            self.refresh_access_token()

            response = self.session.request(
                method=method,
                url=f"{self.base_url}{endpoint}",
                headers=self._headers(),
                timeout=30,
                **kwargs,
            )

        response.raise_for_status()

        return response

    def get(self, endpoint, **kwargs):
        return self._request("GET", endpoint, **kwargs).json()

    def post(self, endpoint, **kwargs):
        return self._request("POST", endpoint, **kwargs).json()

    def put(self, endpoint, **kwargs):
        return self._request("PUT", endpoint, **kwargs).json()

    def patch(self, endpoint, **kwargs):
        return self._request("PATCH", endpoint, **kwargs).json()

    def delete(self, endpoint, **kwargs):
        return self._request("DELETE", endpoint, **kwargs)
    
# singleton instance
client = PossumApiClient()

# usage
# from api_client import client

# data = client.get("/api/observations/")
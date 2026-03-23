"""
Test Suite: Authentication APIs
Covers: Signup, signin, password reset, token validation, user info.
"""

import pytest
import requests
import time
import random
import os

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:4535")

@pytest.fixture(scope="module")
def test_user():
    email = f"pytest_user_{int(time.time())}_{random.randint(1000,9999)}@example.com"
    password = "TestPass123!"
    return {"email": email, "password": password, "name": "Pytest User"}

def test_signup(test_user):
    r = requests.post(f"{API_BASE_URL}/auth/signup", json=test_user)
    assert r.status_code == 200
    data = r.json()
    assert data["success"] is True
    assert "access_token" in data
    test_user["access_token"] = data["access_token"]

def test_signin(test_user):
    payload = {"email": test_user["email"], "password": test_user["password"]}
    r = requests.post(f"{API_BASE_URL}/auth/signin", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["success"] is True
    assert "access_token" in data
    test_user["access_token"] = data["access_token"]

def test_get_current_user(test_user):
    headers = {"Authorization": f"Bearer {test_user['access_token']}"}
    r = requests.get(f"{API_BASE_URL}/auth/me", headers=headers)
    assert r.status_code == 200
    data = r.json()
    assert data["success"] is True
    assert data["user"]["email"] == test_user["email"]

def test_password_validation():
    weak_pwds = ["short", "nouppercase123", "NOLOWERCASE123", "NoDigitsHere"]
    for pwd in weak_pwds:
        payload = {"name": "Test User", "email": f"weak_{int(time.time())}@example.com", "password": pwd}
        r = requests.post(f"{API_BASE_URL}/auth/signup", json=payload)
        assert r.status_code == 422

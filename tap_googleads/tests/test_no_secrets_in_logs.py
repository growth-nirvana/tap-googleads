"""No credential reaches the logs or an error: developer token, OAuth tokens, client secret."""

import logging
from urllib.parse import parse_qs

import pytest
import responses

import tap_googleads.tests.utils as test_utils
from tap_googleads.auth import GoogleAdsAuthenticator
from tap_googleads.client import redact_headers
from tap_googleads.tap import TapGoogleAds

SECRETS = {
    "developer_token": "DEVTOKEN-secret-value",
    "client_secret": "CLIENTSECRET-secret-value",
    "refresh_token": "REFRESHTOKEN-secret-value",
}
ACCESS_TOKEN = "ACCESSTOKEN-secret-value"
CONFIG = {"client_id": "client-123", "customer_ids": "1234567890", **SECRETS}
TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token"


@pytest.fixture(autouse=True)
def _fresh_authenticator():
    # SingletonMeta keeps one authenticator per class across tests.
    GoogleAdsAuthenticator._SingletonMeta__single_instance = None
    TapGoogleAds.write_message = test_utils.accumulate_singer_messages
    del test_utils.SINGER_MESSAGES[:]
    yield
    GoogleAdsAuthenticator._SingletonMeta__single_instance = None


def _assert_clean(text: str) -> None:
    for value in [*SECRETS.values(), ACCESS_TOKEN]:
        assert value not in text


@pytest.mark.parametrize("level", [logging.INFO, logging.DEBUG])
@responses.activate
def test_sync_logs_no_secret(caplog, level):
    caplog.set_level(level)
    responses.add(responses.POST, TOKEN_URL, json={"access_token": ACCESS_TOKEN, "expires_in": 3600})
    responses.add(
        responses.GET,
        "https://googleads.googleapis.com/v26/customers:listAccessibleCustomers",
        json=test_utils.accessible_customer_return_data,
    )
    tap = test_utils.set_up_tap_with_custom_catalog(CONFIG, ["stream_accessible_customers"])
    tap.sync_all()

    _assert_clean("\n".join(r.getMessage() for r in caplog.records))
    token_call = responses.calls[0].request
    # The refresh token and client secret travel in the body, never the URL.
    assert token_call.url == TOKEN_URL
    body = parse_qs(token_call.body)
    assert body["refresh_token"] == [SECRETS["refresh_token"]]
    assert body["client_secret"] == [SECRETS["client_secret"]]
    assert body["grant_type"] == ["refresh_token"]
    # The API call still carries the developer token.
    assert responses.calls[1].request.headers["developer-token"] == SECRETS["developer_token"]


@responses.activate
def test_failed_refresh_error_names_no_secret(caplog):
    responses.add(responses.POST, TOKEN_URL, json={"error": "invalid_grant"}, status=400)
    tap = test_utils.set_up_tap_with_custom_catalog(CONFIG, ["stream_accessible_customers"])
    with pytest.raises(Exception) as raised:
        tap.sync_all()
    assert "invalid_grant" in str(raised.value)
    _assert_clean(str(raised.value) + "\n".join(r.getMessage() for r in caplog.records))


def test_redact_headers():
    headers = {"developer-token": "x", "Authorization": "Bearer y", "login-customer-id": "1", "User-Agent": "ua"}
    assert redact_headers(headers) == {
        "developer-token": "<redacted>",
        "Authorization": "<redacted>",
        "login-customer-id": "<redacted>",
        "User-Agent": "ua",
    }

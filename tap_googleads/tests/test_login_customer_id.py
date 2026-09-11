"""Tests for context-aware login-customer-id header selection."""

import unittest
from unittest.mock import MagicMock, patch

from singer_sdk.streams import RESTStream

from tap_googleads.streams import CampaignsStream
from tap_googleads.tap import TapGoogleAds


class TestLoginCustomerId(unittest.TestCase):
    """Test login-customer-id resolution per request context."""

    def setUp(self):
        self.base_config = {
            "client_id": "1234",
            "client_secret": "1234",
            "refresh_token": "1234",
            "developer_token": "1234",
            "start_date": "2024-01-01",
        }

    def _campaigns_stream(self, config):
        tap = TapGoogleAds(config={**self.base_config, **config})
        return CampaignsStream(tap)

    def test_explicit_login_customer_id_overrides_context(self):
        stream = self._campaigns_stream(
            {
                "customer_ids": "1111111111,2222222222",
                "login_customer_id": "9999999999",
            }
        )
        self.assertEqual(
            stream.get_login_customer_id({"customer_id": "2222222222"}),
            "9999999999",
        )

    def test_uses_partition_customer_id_without_login_customer_id(self):
        stream = self._campaigns_stream({"customer_ids": "1111111111,2222222222"})
        self.assertEqual(
            stream.get_login_customer_id({"customer_id": "2222222222"}),
            "2222222222",
        )
        self.assertEqual(
            stream.get_login_customer_id({"customer_id": "1111111111"}),
            "1111111111",
        )

    def test_falls_back_to_first_customer_id_without_context(self):
        stream = self._campaigns_stream({"customer_ids": "1111111111,2222222222"})
        self.assertEqual(stream.get_login_customer_id(None), "1111111111")

    def test_prepare_request_sets_context_login_header(self):
        stream = self._campaigns_stream({"customer_ids": "1111111111,2222222222"})
        mock_prepared = MagicMock()
        mock_prepared.headers = {}
        with patch.object(RESTStream, "prepare_request", return_value=mock_prepared):
            prepared = stream.prepare_request({"customer_id": "2222222222"}, None)
        self.assertEqual(prepared.headers["login-customer-id"], "2222222222")

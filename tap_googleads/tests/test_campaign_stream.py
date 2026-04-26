"""Tests for the campaign resource stream (stream_campaign) GAQL and schema."""

import json
import unittest
from pathlib import Path

from tap_googleads.streams import SCHEMAS_DIR, CampaignsStream
from tap_googleads.tap import TapGoogleAds


def _minimal_tap_config() -> dict:
    return {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh",
        "developer_token": "test_dev_token",
    }


class TestCampaignsStreamBudgetFields(unittest.TestCase):
    """Ensure campaign stream selects budget fields and schema matches."""

    def setUp(self) -> None:
        self.tap = TapGoogleAds(config=_minimal_tap_config())
        self.stream = CampaignsStream(tap=self.tap)

    def test_gaql_includes_campaign_budget_fields(self) -> None:
        q = self.stream.gaql.lower()
        for fragment in (
            "campaign.campaign_budget",
            "campaign_budget.resource_name",
            "campaign_budget.id",
            "campaign_budget.name",
            "campaign_budget.amount_micros",
            "campaign_budget.total_amount_micros",
            "campaign_budget.period",
            "campaign_budget.status",
            "campaign_budget.explicitly_shared",
            "campaign_budget.reference_count",
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, q)

    def test_schema_includes_flattened_budget_properties(self) -> None:
        path = Path(SCHEMAS_DIR) / "campaign.json"
        with open(path, encoding="utf-8") as f:
            schema = json.load(f)
        props = schema["properties"]
        for key in (
            "campaign__campaignBudget",
            "campaignBudget__resourceName",
            "campaignBudget__id",
            "campaignBudget__name",
            "campaignBudget__amountMicros",
            "campaignBudget__totalAmountMicros",
            "campaignBudget__period",
            "campaignBudget__status",
            "campaignBudget__explicitlyShared",
            "campaignBudget__referenceCount",
        ):
            with self.subTest(key=key):
                self.assertIn(key, props)


if __name__ == "__main__":
    unittest.main()

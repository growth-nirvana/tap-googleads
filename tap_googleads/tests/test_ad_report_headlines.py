"""Tests for ad headline/description resolution on stream_ad_report."""

import json
import unittest
from pathlib import Path

from tap_googleads.streams import (
    SCHEMAS_DIR,
    AdReportStream,
    _resolve_ad_description,
    _resolve_ad_headline,
)
from tap_googleads.tap import TapGoogleAds


def _minimal_tap_config() -> dict:
    return {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh",
        "developer_token": "test_dev_token",
        "start_date": "2025-01-01",
    }


RSA_AD = {
    "type": "RESPONSIVE_SEARCH_AD",
    "responsiveSearchAd": {
        "headlines": [
            {"pinnedField": "HEADLINE_2", "text": "Shop Truck Chrome"},
            {"pinnedField": "HEADLINE_1", "text": "Semi Truck Parts"},
        ],
        "descriptions": [
            {"pinnedField": "DESCRIPTION_2", "text": "Chrome accessories for every semi truck."},
            {"pinnedField": "DESCRIPTION_1", "text": "Shop premium truck parts and accessories."},
        ],
    },
}


class TestAdReportHeadlines(unittest.TestCase):
    def setUp(self) -> None:
        self.tap = TapGoogleAds(config=_minimal_tap_config())
        self.stream = AdReportStream(tap=self.tap)

    def test_gaql_includes_responsive_search_ad_fields(self) -> None:
        q = self.stream.gaql.lower()
        for fragment in (
            "ad_group_ad.ad.responsive_search_ad.headlines",
            "ad_group_ad.ad.responsive_search_ad.descriptions",
            "ad_group_ad.ad.responsive_display_ad.headlines",
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, q)

    def test_resolve_rsa_headline_prefers_pinned_headline_1(self) -> None:
        self.assertEqual(_resolve_ad_headline(RSA_AD), "Semi Truck Parts")

    def test_resolve_rsa_description_prefers_pinned_description_1(self) -> None:
        self.assertEqual(
            _resolve_ad_description(RSA_AD),
            "Shop premium truck parts and accessories.",
        )

    def test_post_process_adds_headline_and_description(self) -> None:
        row = {"adGroupAd": {"ad": RSA_AD}}
        processed = self.stream.post_process(row, None)
        self.assertEqual(processed["headline"], "Semi Truck Parts")
        self.assertEqual(
            processed["description"],
            "Shop premium truck parts and accessories.",
        )

    def test_schema_includes_headline_and_rsa_fields(self) -> None:
        path = Path(SCHEMAS_DIR) / "ad_report.json"
        with open(path, encoding="utf-8") as f:
            schema = json.load(f)
        props = schema["properties"]
        for key in (
            "headline",
            "description",
            "adGroupAd__ad__responsiveSearchAd__headlines",
            "adGroupAd__ad__responsiveSearchAd__descriptions",
        ):
            with self.subTest(key=key):
                self.assertIn(key, props)


if __name__ == "__main__":
    unittest.main()

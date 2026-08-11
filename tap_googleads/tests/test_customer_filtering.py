"""Tests for customer filtering logic to ensure all configured customers are processed."""

import pytest
from tap_googleads.streams import AccessibleCustomers, CustomerHierarchyStream
from tap_googleads.client import GoogleAdsStream


class TestAccessibleCustomersFiltering:
    """Test that AccessibleCustomers uses configured customer_ids directly."""

    def test_uses_configured_customer_ids_directly(self):
        """Test that configured customer_ids are used directly, not filtered from accessible list."""
        # Mock config with customer_ids
        config = {
            "customer_ids": "3320345197,6441860864,9127321743",
            "developer_token": "test",
            "refresh_token": "test",
            "client_id": "test",
            "client_secret": "test",
            "start_date": "2026-02-01",
            "end_date": "2026-02-28",
        }
        
        # Create stream instance
        from tap_googleads.tap import TapGoogleAds
        tap = TapGoogleAds(config=config)
        stream = AccessibleCustomers(tap=tap)
        
        # Mock record with accessible customers that DON'T include 3320345197
        # (simulating the case where login_customer_id scoping excludes it)
        record = {
            "resourceNames": [
                "customers/6441860864",
                "customers/9127321743",
                "customers/9999999999",  # Not in config
            ]
        }
        
        # Get child context
        context = stream.get_child_context(record, {})
        
        # Should use configured customer_ids directly, including 3320345197
        assert "customer_ids" in context
        assert "3320345197" in context["customer_ids"]
        assert "6441860864" in context["customer_ids"]
        assert "9127321743" in context["customer_ids"]
        assert "9999999999" not in context["customer_ids"]  # Not in config

    def test_uses_accessible_customers_when_no_config(self):
        """Test that accessible customers are used when customer_ids not configured."""
        config = {
            "developer_token": "test",
            "refresh_token": "test",
            "client_id": "test",
            "client_secret": "test",
            "start_date": "2026-02-01",
            "end_date": "2026-02-28",
        }
        
        from tap_googleads.tap import TapGoogleAds
        tap = TapGoogleAds(config=config)
        stream = AccessibleCustomers(tap=tap)
        
        record = {
            "resourceNames": [
                "customers/6441860864",
                "customers/9127321743",
            ]
        }
        
        context = stream.get_child_context(record, {})
        
        # Should use all accessible customers
        assert "customer_ids" in context
        assert "6441860864" in context["customer_ids"]
        assert "9127321743" in context["customer_ids"]


class TestCustomerHierarchyFiltering:
    """Test that CustomerHierarchyStream correctly processes configured customers."""

    def test_customer_id_included_in_family_line(self):
        """Test that customer_id is always included in family_line for intersection check."""
        config = {
            "customer_ids": "3320345197",
            "developer_token": "test",
            "refresh_token": "test",
            "client_id": "test",
            "client_secret": "test",
            "start_date": "2026-02-01",
            "end_date": "2026-02-28",
        }
        
        from tap_googleads.tap import TapGoogleAds
        tap = TapGoogleAds(config=config)
        stream = CustomerHierarchyStream(tap=tap)
        
        # Test with resourceName that might not include customer_id in family_line
        record = {
            "customer_id": "3320345197",
            "id": "3320345197",
            "manager": False,
            "status": "ENABLED",
            "resourceName": "customers/3320345197",  # Direct customer resource
        }
        
        context = stream.get_child_context(record, {})
        
        # Should create child context for configured customer
        assert context is not None
        assert context["customer_id"] == "3320345197"

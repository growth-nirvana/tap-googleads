"""REST client handling, including GoogleAdsStream base class."""

from datetime import datetime
from backports.cached_property import cached_property
from typing import Any, Dict, Optional
import uuid

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream

from tap_googleads.auth import GoogleAdsAuthenticator, ProxyGoogleAdsAuthenticator

from pendulum import parse
import typing as t
import singer_sdk._singerlib as singer
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._typing import (
    conform_record_data_types,
)
from singer_sdk.helpers._util import utc_now


class ResumableAPIError(Exception):
    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


class GoogleAdsStream(RESTStream):
    """GoogleAds stream class."""

    url_base = "https://googleads.googleapis.com/v18"
    rest_method = "POST"
    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.nextPageToken"  # Or override `get_next_page_token`.
    _LOG_REQUEST_METRIC_URLS: bool = True
    
    # Class variable to store the run_id shared across all streams
    _shared_run_id = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.config.get("customer_id"):
            self._config["customer_ids"] = [_sanitise_customer_id(self.config.get("customer_id"))]
        elif self.config.get("customer_ids"):
            raw_customer_ids = self.config.get("customer_ids").split(",")
            self._config["customer_ids"] = list(map(_sanitise_customer_id, raw_customer_ids))
        
        # Generate a unique run_id for this tap run if not already set
        if GoogleAdsStream._shared_run_id is None:
            GoogleAdsStream._shared_run_id = int(datetime.now().timestamp() * 1000)

    @property
    def run_id(self) -> int:
        """Return the unique run_id for this tap run."""
        return GoogleAdsStream._shared_run_id

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A :class:`requests.Response` object.

        Returns:
            str: The error message
        """
        base_msg = super().response_error_message(response)
        try:
            error = response.json()["error"]
            main_message = (
                f"Error {error['code']}: {error['message']} ({error['status']})"
            )

            if "details" in error and error["details"]:
                detail = error["details"][0]
                if "errors" in detail and detail["errors"]:
                    error_detail = detail["errors"][0]
                    detailed_message = error_detail.get("message", "")
                    request_id = detail.get("requestId", "")

                    return f"{base_msg}. {main_message}\nDetails: {detailed_message}\nRequest ID: {request_id}"

            return base_msg + main_message
        except Exception:
            return base_msg

    @cached_property
    def authenticator(self) -> OAuthAuthenticator:
        """Return a new authenticator object."""
        base_auth_url = "https://www.googleapis.com/oauth2/v4/token"
        # Silly way to do parameters but it works

        client_id = self.config.get("client_id", None)
        client_secret = self.config.get(
            "client_secret", None
        )
        refresh_token = self.config.get(
            "refresh_token", None
        )

        auth_url = base_auth_url + f"?refresh_token={refresh_token}"
        auth_url = auth_url + f"&client_id={client_id}"
        auth_url = auth_url + f"&client_secret={client_secret}"
        auth_url = auth_url + "&grant_type=refresh_token"

        if client_id and client_secret and refresh_token:
            return GoogleAdsAuthenticator(stream=self, auth_endpoint=auth_url)

        auth_body = {}
        auth_headers = {}

        auth_body["refresh_token"] = self.config.get("refresh_token")
        auth_body["grant_type"] = "refresh_token"

        auth_headers["authorization"] = self.config.get("refresh_proxy_url_auth")
        auth_headers["Content-Type"] = "application/json"
        auth_headers["Accept"] = "application/json"

        return ProxyGoogleAdsAuthenticator(
            stream=self,
            auth_endpoint=self.config.get("refresh_proxy_url"),
            auth_body=auth_body,
            auth_headers=auth_headers,
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["developer-token"] = self.config["developer_token"]
        
        if self.login_customer_id:
            headers["login-customer-id"] = self.login_customer_id
            
        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def get_records(self, context):
        """Get records from the stream."""
        self.logger.info("[DEBUG] Starting get_records for stream: %s", self.name)
        try:
            for record in super().get_records(context):
                yield record
        except ResumableAPIError as e:
            self.logger.error("[DEBUG] ResumableAPIError in get_records: %s", str(e))
            self.logger.warning(e)
        except Exception as e:
            self.logger.error("[DEBUG] Unexpected error in get_records: %s", str(e))
            raise

    @property
    def gaql(self):
        raise NotImplementedError

    @property
    def path(self) -> str:
        # Paramas
        path = "/customers/{customer_id}/googleAds:search?query="
        return path + self.gaql

    @cached_property
    def start_date(self):
        try:
            return datetime.fromisoformat(self.config["start_date"]).strftime(r"'%Y-%m-%d'")
        except Exception:
            return datetime.strptime(self.config["start_date"], "%Y-%m-%dT%H:%M:%SZ").strftime(r"'%Y-%m-%d'")

    @cached_property
    def end_date(self):
        return parse(self.config["end_date"]).strftime(r"'%Y-%m-%d'") if self.config.get("end_date") else datetime.now().strftime(r"'%Y-%m-%d'")

    @cached_property
    def customer_ids(self):
        customer_ids = self.config.get("customer_ids")
        customer_id = self.config.get("customer_id")

        if customer_ids is None:
            if customer_id is None:
                return
            customer_ids = [customer_id]

        return list(map(_sanitise_customer_id, customer_ids))

    @cached_property
    def login_customer_id(self):
        """Always return the top-level MCC account ID."""
        # If login_customer_id is explicitly set in config, use that
        if self.config.get("login_customer_id"):
            return _sanitise_customer_id(self.config.get("login_customer_id"))
        
        # Otherwise, use the first customer ID as the MCC account
        # This assumes the first ID in the list is the MCC account
        customer_ids = self.customer_ids
        if customer_ids and len(customer_ids) > 0:
            return customer_ids[0]
        
        return None


    def _generate_record_messages(
        self,
        record: dict,
    ) -> t.Generator[singer.RecordMessage, None, None]:
        """Write out a RECORD message.

        Args:
            record: A single stream record.

        Yields:
            Record message objects.
        """
        # Add run_id to every record
        record['run_id'] = self.run_id
        
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            pop_deselected_record_properties(mapped_record, self.schema, self.mask, self.logger)
            mapped_record = conform_record_data_types(
                stream_name=self.name,
                record=mapped_record,
                schema=self.schema,
                level=self.TYPE_CONFORMANCE_LEVEL,
                logger=self.logger,
            )
            # Emit record if not filtered
            if mapped_record is not None:
                yield singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )

    def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any]) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST request.
        """
        if hasattr(self, 'path') and 'googleAds:search' in self.path:
            try:
                return {"query": self.gaql}
            except NotImplementedError:
                return None
        return None

    def request_records(self, context):
        """Override to log outgoing HTTP request details for debugging."""
        self.logger.info("[DEBUG] ===== Starting request_records for stream: %s =====", self.name)
        
        # Prepare request details
        url = self.get_url(context)
        method = self.rest_method
        headers = self.http_headers
        params = self.get_url_params(context, None)
        body = getattr(self, 'body', None)
        if callable(body):
            body = body(context)
        
        # Log request details
        self.logger.info("[DEBUG] Request Details:")
        self.logger.info("[DEBUG] Stream Name: %s", self.name)
        self.logger.info("[DEBUG] Processing customer_id: %s", context.get('customer_id') if context else None)
        self.logger.info("[DEBUG] Using login_customer_id: %s", self.login_customer_id)
        self.logger.info("[DEBUG] URL: %s", url)
        self.logger.info("[DEBUG] Method: %s", method)
        self.logger.info("[DEBUG] Headers: %s", headers)
        self.logger.info("[DEBUG] Params: %s", params)
        self.logger.info("[DEBUG] Body: %s", body)
        
        # Only try to log GAQL for streams that use it
        if hasattr(self, 'path') and 'googleAds:search' in self.path:
            try:
                self.logger.info("[DEBUG] GAQL Query: %s", self.gaql)
            except NotImplementedError:
                self.logger.info("[DEBUG] GAQL Query: Not implemented for this stream")
        
        # Make the request
        self.logger.info("[DEBUG] Making API request...")
        try:
            response = super().request_records(context)
            self.logger.info("[DEBUG] Request completed successfully")
            
            # Log response details if available
            if hasattr(response, 'response'):
                self.logger.info("[DEBUG] Response Details:")
                self.logger.info("[DEBUG] Status Code: %s", response.response.status_code)
                self.logger.info("[DEBUG] Headers: %s", response.response.headers)
                try:
                    self.logger.info("[DEBUG] Response Body: %s", response.response.json())
                except:
                    self.logger.info("[DEBUG] Response Body: %s", response.response.text)
            else:
                self.logger.info("[DEBUG] No response object available")
            
            return response
        except Exception as e:
            self.logger.error("[DEBUG] Error making request: %s", str(e))
            raise
        finally:
            self.logger.info("[DEBUG] ===== Completed request_records for stream: %s =====", self.name)

def _sanitise_customer_id(customer_id: str):
    return customer_id.replace("-", "").strip()

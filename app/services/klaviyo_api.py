import httpx
import asyncio
import base64
from typing import Dict, Any, List, Optional, AsyncGenerator, Callable, Literal
from datetime import datetime
import logging
from ..core.config import settings

logger = logging.getLogger(__name__)

# Constants matching Node.js version
BASE_URL = "https://a.klaviyo.com/api/"
REVISION = "2025-04-15"


def get_klaviyo_headers(api_key: str) -> Dict[str, str]:
    """Returns the correct headers for Klaviyo API requests."""
    headers = {
        "revision": REVISION,
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/json",
    }
    
    if api_key and api_key.startswith("pk_"):
        headers["Authorization"] = f"Klaviyo-API-Key {api_key}"
    else:
        headers["Authorization"] = f"Bearer {api_key}"
    
    return headers


async def klaviyo_request(
    method: Literal["GET", "POST", "PATCH", "DELETE"],
    endpoint: str,
    api_key: str,
    payload: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Generic Klaviyo API request function."""
    url = endpoint if endpoint.startswith("http") else f"{BASE_URL}{endpoint}"
    headers = get_klaviyo_headers(api_key)
    
    async with httpx.AsyncClient(timeout=60.0) as client:  # 60 second timeout for large requests
        if payload and method in ["POST", "PATCH"]:
            response = await client.request(method, url, headers=headers, json=payload)
        else:
            response = await client.request(method, url, headers=headers)
        
        if response.status_code >= 400:
            error_text = response.text
            raise Exception(f"Klaviyo API error: {response.status_code} {error_text}")
        
        return response.json()


# Convenience wrappers
async def klaviyo_get(endpoint: str, api_key: str) -> Dict[str, Any]:
    return await klaviyo_request("GET", endpoint, api_key)


async def klaviyo_post(endpoint: str, payload: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    return await klaviyo_request("POST", endpoint, api_key, payload)


async def klaviyo_patch(endpoint: str, payload: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    return await klaviyo_request("PATCH", endpoint, api_key, payload)


async def klaviyo_delete(endpoint: str, api_key: str) -> Dict[str, Any]:
    return await klaviyo_request("DELETE", endpoint, api_key)


async def klaviyo_get_all(endpoint: str, api_key: str) -> Dict[str, Any]:
    """Fetches all paginated results from a Klaviyo endpoint."""
    url = endpoint
    all_data = []
    all_included = []
    first_response = None
    
    while url:
        res = await klaviyo_get(url, api_key)
        if not first_response:
            first_response = res
        
        if isinstance(res.get("data"), list):
            all_data.extend(res["data"])
        
        if isinstance(res.get("included"), list):
            all_included.extend(res["included"])
        
        url = res.get("links", {}).get("next")
    
    # Return a combined response
    return {
        **first_response,
        "data": all_data,
        "included": all_included,
        "links": first_response.get("links", {})
    }


async def klaviyo_report_post(endpoint: str, payload: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Post to report endpoints and handle paginated results."""
    url = endpoint
    all_data = []
    all_included = []
    all_results = []
    first_response = None
    
    while url:
        res = await klaviyo_post(url, payload, api_key)
        if not first_response:
            first_response = res
        
        if isinstance(res.get("data"), list):
            all_data.extend(res["data"])
        
        if isinstance(res.get("included"), list):
            all_included.extend(res["included"])
        
        # Concatenate results arrays if present
        if (res.get("data") and 
            res["data"].get("attributes") and 
            isinstance(res["data"]["attributes"].get("results"), list)):
            all_results.extend(res["data"]["attributes"]["results"])
        
        url = res.get("links", {}).get("next")
    
    # Attach the combined results to the returned data
    if first_response and first_response.get("data") and first_response["data"].get("attributes"):
        first_response["data"]["attributes"]["results"] = all_results
    
    return {
        **first_response,
        "data": first_response.get("data"),
        "included": all_included,
        "links": first_response.get("links", {})
    }


class KlaviyoAPIClient:
    BASE_URL = "https://a.klaviyo.com/api"
    TOKEN_URL = "https://a.klaviyo.com/oauth/token"
    
    def __init__(
        self, 
        credential: str, 
        refresh_token: Optional[str] = None,
        token_refresh_callback: Optional[Callable[[str, str], None]] = None
    ):
        self.credential = credential
        self.refresh_token = refresh_token
        self.token_refresh_callback = token_refresh_callback
        self.is_oauth = not credential.startswith('pk_')
        
        self._update_headers()
        self.client = httpx.AsyncClient(timeout=30.0)
    
    def _update_headers(self):
        """Update headers with current credential"""
        if self.is_oauth:
            # OAuth token - use Bearer authorization
            self.headers = {
                "Authorization": f"Bearer {self.credential}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "revision": "2024-10-15"
            }
        else:
            # API Key - use Klaviyo-API-Key authorization
            self.headers = {
                "Authorization": f"Klaviyo-API-Key {self.credential}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "revision": "2024-10-15"
            }
    
    async def _refresh_access_token(self) -> bool:
        """Refresh the OAuth access token using the refresh token"""
        if not self.is_oauth or not self.refresh_token:
            return False
        
        if not settings.KLAVIYO_CLIENT_ID or not settings.KLAVIYO_CLIENT_SECRET:
            logger.error("Klaviyo OAuth client credentials not configured")
            return False
        
        # Create Basic auth header for client credentials
        credentials = f"{settings.KLAVIYO_CLIENT_ID}:{settings.KLAVIYO_CLIENT_SECRET}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }
        
        try:
            response = await self.client.post(self.TOKEN_URL, headers=headers, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                new_access_token = token_data.get("access_token")
                new_refresh_token = token_data.get("refresh_token", self.refresh_token)
                
                if new_access_token:
                    self.credential = new_access_token
                    self.refresh_token = new_refresh_token
                    self._update_headers()
                    
                    # Call callback to update store if provided
                    if self.token_refresh_callback:
                        await self.token_refresh_callback(new_access_token, new_refresh_token)
                    
                    logger.info("Successfully refreshed Klaviyo OAuth token")
                    return True
            
            elif response.status_code == 400:
                error_data = response.json()
                if error_data.get("error") == "invalid_grant":
                    logger.error("Klaviyo refresh token is invalid - integration needs re-authorization")
                    return False
            
            logger.error(f"Failed to refresh token: {response.status_code} - {response.text}")
            return False
            
        except Exception as e:
            logger.error(f"Error refreshing Klaviyo token: {str(e)}")
            return False
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        retry_on_auth_error: bool = True
    ) -> Dict[str, Any]:
        """Make a request to Klaviyo API with rate limiting and automatic token refresh"""
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            # Apply rate limiting
            await asyncio.sleep(settings.KLAVIYO_RATE_LIMIT_DELAY)
            
            response = await self.client.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=json_data
            )
            
            # Check for 401 authentication error
            if response.status_code == 401 and retry_on_auth_error and self.is_oauth:
                error_data = response.json() if response.content else {}
                errors = error_data.get("errors", [])
                
                # Check if it's an authentication error that might be resolved by token refresh
                if any(error.get("code") == "not_authenticated" for error in errors):
                    logger.info("Received 401 error, attempting to refresh OAuth token")
                    
                    if await self._refresh_access_token():
                        # Retry the request with new token (prevent infinite recursion)
                        return await self._make_request(
                            method, endpoint, params, json_data, retry_on_auth_error=False
                        )
                    else:
                        logger.error("Failed to refresh token, authentication error persists")
            
            response.raise_for_status()
            return response.json()
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error from Klaviyo API: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error making request to Klaviyo API: {str(e)}")
            raise
    
    async def get_all_pages(
        self,
        endpoint: str,
        params: Optional[Dict] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Get all pages from a paginated endpoint"""
        if params is None:
            params = {}
        
        has_more = True
        next_cursor = None
        
        while has_more:
            if next_cursor:
                params["page[cursor]"] = next_cursor
            
            response = await self._make_request("GET", endpoint, params=params)
            
            yield response
            
            # Check for next page
            links = response.get("links", {})
            next_link = links.get("next")
            
            if next_link:
                # Extract cursor from next link
                import urllib.parse
                parsed = urllib.parse.urlparse(next_link)
                query_params = urllib.parse.parse_qs(parsed.query)
                next_cursor = query_params.get("page[cursor]", [None])[0]
                has_more = bool(next_cursor)
            else:
                has_more = False
    
    async def get_campaigns(
        self,
        filter_params: Optional[Dict] = None,
        include: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get all campaigns with optional filters"""
        params = {}
        
        if filter_params:
            filter_str = ",".join([f"{k},{v}" for k, v in filter_params.items()])
            params["filter"] = filter_str
        
        if include:
            params["include"] = ",".join(include)
        
        all_campaigns = []
        async for page in self.get_all_pages("/campaigns", params):
            campaigns = page.get("data", [])
            all_campaigns.extend(campaigns)
        
        return all_campaigns
    
    async def get_campaign_metrics(
        self,
        campaign_id: str,
        metric_names: List[str]
    ) -> Dict[str, Any]:
        """Get aggregated metrics for a campaign"""
        endpoint = "/reporting/campaign-values-reports"
        
        params = {
            "filter": f"equals(campaign_id,'{campaign_id}')",
            "fields[campaign-values-report]": ",".join(metric_names),
            "statistics": ",".join(metric_names)
        }
        
        response = await self._make_request("GET", endpoint, params=params)
        return response.get("data", {})
    
    async def get_flows(self) -> List[Dict[str, Any]]:
        """Get all flows"""
        all_flows = []
        async for page in self.get_all_pages("/flows"):
            flows = page.get("data", [])
            all_flows.extend(flows)
        
        return all_flows
    
    async def get_flow_messages(self, flow_id: str) -> List[Dict[str, Any]]:
        """Get all messages for a flow"""
        endpoint = f"/flows/{flow_id}/relationships/flow-messages"
        response = await self._make_request("GET", endpoint)
        return response.get("data", [])
    
    async def get_flow_message_metrics(
        self,
        flow_id: str,
        message_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get metrics for a flow message"""
        endpoint = "/reporting/flow-series-reports"
        
        params = {
            "filter": f"equals(flow_id,'{flow_id}'),equals(flow_message_id,'{message_id}')",
            "fields[flow-series-report]": "opens,clicks,delivered,bounced,unsubscribes,spam_complaints,conversions,conversion_value",
            "statistics": "opens,clicks,delivered,bounced,unsubscribes,spam_complaints,conversions,conversion_value"
        }
        
        if start_date:
            params["filter"] += f",greater-or-equal(datetime,{start_date.isoformat()})"
        if end_date:
            params["filter"] += f",less-or-equal(datetime,{end_date.isoformat()})"
        
        response = await self._make_request("GET", endpoint, params=params)
        return response.get("data", {})
    
    async def get_lists(self) -> List[Dict[str, Any]]:
        """Get all lists"""
        all_lists = []
        async for page in self.get_all_pages("/lists"):
            lists = page.get("data", [])
            all_lists.extend(lists)
        
        return all_lists
    
    async def get_segments(self) -> List[Dict[str, Any]]:
        """Get all segments"""
        all_segments = []
        async for page in self.get_all_pages("/segments"):
            segments = page.get("data", [])
            all_segments.extend(segments)
        
        return all_segments
    
    async def get_tags(self) -> List[Dict[str, Any]]:
        """Get all tags"""
        all_tags = []
        async for page in self.get_all_pages("/tags"):
            tags = page.get("data", [])
            all_tags.extend(tags)
        
        return all_tags


async def create_klaviyo_client(
    api_key: Optional[str] = None, 
    oauth_token: Optional[str] = None,
    refresh_token: Optional[str] = None
) -> KlaviyoAPIClient:
    """Create a Klaviyo API client instance with either API key or OAuth token"""
    credential = oauth_token or api_key or settings.KLAVIYO_API_KEY
    if not credential:
        raise ValueError("Klaviyo API key or OAuth token is required")
    return KlaviyoAPIClient(credential, refresh_token)


async def create_klaviyo_client_from_store(store) -> KlaviyoAPIClient:
    """Create a Klaviyo API client instance from store integration settings"""
    oauth_token = store.klaviyo_integration.oauth_token
    # Support both apiKey and api_key field names
    api_key = store.klaviyo_integration.apiKey or store.klaviyo_integration.api_key
    refresh_token = store.klaviyo_integration.refresh_token
    
    credential = oauth_token or api_key
    if not credential:
        raise ValueError("Store must have either Klaviyo API key or OAuth token configured")
    
    # Create callback to update store when tokens are refreshed
    async def token_refresh_callback(new_access_token: str, new_refresh_token: str):
        store.klaviyo_integration.oauth_token = new_access_token
        store.klaviyo_integration.refresh_token = new_refresh_token
        await store.save()
        logger.info(f"Updated OAuth tokens for store {store.public_id}")
    
    return KlaviyoAPIClient(credential, refresh_token, token_refresh_callback)


async def get_flows_with_rate_limit(
    flow_ids: List[str], 
    api_key: str,
    max_concurrent: int = 2,
    delay_ms: int = 1000
) -> List[Dict[str, Any]]:
    """
    Rate-limited helper function to get multiple flows.
    Respects Klaviyo's rate limits: 3/s burst, 60/m steady.
    """
    results = []
    errors = []
    
    # Process flows in batches to respect rate limits
    for i in range(0, len(flow_ids), max_concurrent):
        batch = flow_ids[i:i + max_concurrent]
        batch_tasks = []
        
        for idx, flow_id in enumerate(batch):
            # Add small delay between requests in the same batch
            if idx > 0:
                await asyncio.sleep(0.35)  # ~3/s rate limit
            
            batch_tasks.append(get_flow(flow_id, api_key))
        
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for flow_id, result in zip(batch, batch_results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching flow {flow_id}: {result}")
                errors.append({"flow_id": flow_id, "error": str(result)})
            elif result:
                results.append(result)
        
        # Add delay between batches to respect steady rate limit
        if i + max_concurrent < len(flow_ids):
            await asyncio.sleep(delay_ms / 1000)
    
    if errors:
        logger.warning(f"Failed to fetch {len(errors)} flows: {errors}")
    
    return results


async def get_flow(flow_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Get a single flow by ID with error handling.
    """
    try:
        response = await klaviyo_get(f"flows/{flow_id}", api_key)
        return response
    except Exception as e:
        logger.error(f"Error fetching flow {flow_id}: {e}")
        return None
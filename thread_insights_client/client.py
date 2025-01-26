import os
import re
import json
import requests
import pandas as pd

from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

class ThreadsInsights:
    """
    A client for interacting with the Threads API:
    - Token exchanges
    - Insights retrieval
    - Thread fetching and conversions to DataFrame
    """

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None
    ):
        """
        Initialize the ThreadsClient, loading .env if available.
        If client_id or client_secret are not provided, the client
        will attempt to read them from environment variables.
        """
        load_dotenv()

        self.client_id = client_id or os.getenv("CLIENT_ID")
        self.client_secret = client_secret or os.getenv("CLIENT_SECRET")
        # You can also allow a custom redirect_uri
        self.redirect_uri = redirect_uri or "https://oauth.pstmn.io/v1/browser-callback"

        # You might store tokens in the instance once retrieved
        self.short_lived_token = None
        self.long_lived_token = None

    # ---------------------
    # Private helper to build error
    # ---------------------
    def _build_error(self, message: str, code: Optional[str] = None) -> Dict[str, Any]:
        error_dict = {"error": {"message": message}}
        if code:
            error_dict["error"]["code"] = code
        return error_dict

    # ---------------------
    # 1) Exchange Code for Token
    # ---------------------
    def exchange_code_for_token(self, redirect_url: str) -> Dict[str, Union[str, int, None, Dict[str, Any]]]:
        """
        Extract the authorization code from a redirect URL and exchange it for a short-lived token.
        """
        parsed_url = urlparse(redirect_url)
        query_params = parse_qs(parsed_url.query)
        auth_code = query_params.get("code", [None])[0]

        if not auth_code:
            return self._build_error("Authorization code not found in the URL.", code="MISSING_CODE")

        if not self.client_id or not self.client_secret:
            return self._build_error("Missing 'CLIENT_ID' or 'CLIENT_SECRET'.", code="MISSING_ENV")

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.redirect_uri,
        }

        try:
            response = requests.post("https://graph.threads.net/oauth/access_token", data=data)
            if response.status_code == 200:
                token_info = response.json()
                self.short_lived_token = token_info.get("access_token")
                return {
                    "short_lived_token": self.short_lived_token,
                    "token_type": token_info.get("token_type"),
                    "expires_in": token_info.get("expires_in"),
                    "error": None
                }
            else:
                return self._build_error(
                    message=str(response.json()),
                    code="TOKEN_EXCHANGE_FAILED"
                )
        except requests.exceptions.RequestException as e:
            return self._build_error(f"Request failed: {e}", code="REQUEST_FAILED")

    # ---------------------
    # 2) Get Long-Lived Token
    # ---------------------
    def get_long_lived_token(self, short_lived_token: Optional[str] = None) -> Dict[str, Optional[Any]]:
        """
        Exchange a short-lived token for a long-lived token.
        """
        token_to_use = short_lived_token or self.short_lived_token
        if not token_to_use:
            return self._build_error("No short-lived token provided.", code="MISSING_TOKEN")

        if not self.client_secret:
            return self._build_error("Missing 'CLIENT_SECRET' in environment variables.", code="MISSING_ENV")

        params = {
            "grant_type": "th_exchange_token",
            "client_secret": self.client_secret,
            "access_token": token_to_use,
        }

        try:
            resp = requests.get("https://graph.threads.net/access_token", params=params)
            if resp.status_code == 200:
                info = resp.json()
                self.long_lived_token = info.get("access_token")
                return {
                    "long_lived_token": self.long_lived_token,
                    "error": None
                }
            else:
                return self._build_error(
                    message=str(resp.json()),
                    code="LONG_TOKEN_EXCHANGE_FAILED"
                )
        except requests.exceptions.RequestException as e:
            return self._build_error(f"Request failed: {e}", code="REQUEST_FAILED")

    # ---------------------
    # 3) Get Threads User Insights
    # ---------------------
    def get_threads_user_insights(
        self,
        access_token: str,
        metrics: List[str],
        since: Optional[int] = None,
        until: Optional[int] = None,
        breakdown: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch Threads user insights.
        """
        VALID_METRICS = [
            "likes", "replies", "followers_count", "follower_demographics",
            "reposts", "views", "quotes"
        ]
        invalid_metrics = [m for m in metrics if m not in VALID_METRICS]
        if invalid_metrics:
            return self._build_error(
                f"Invalid metric(s): {', '.join(invalid_metrics)}. Valid metrics: {', '.join(VALID_METRICS)}",
                code="INVALID_INPUT"
            )

        params = {"metric": ",".join(metrics)}
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if breakdown:
            if breakdown not in ["country", "city", "age", "gender"]:
                return self._build_error(
                    "Invalid breakdown value. Must be 'country', 'city', 'age', or 'gender'.",
                    code="INVALID_INPUT"
                )
            params["breakdown"] = breakdown

        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            response = requests.get("https://graph.threads.net/me/threads_insights", params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                return self._build_error(str(response.json()), code="INSIGHTS_FETCH_FAILED")
        except requests.exceptions.RequestException as e:
            return self._build_error(f"Request failed: {e}", code="REQUEST_FAILED")

    # ---------------------
    # 4) Get Media Insights
    # ---------------------
    def get_media_insights(
        self,
        access_token: str,
        media_id: str,
        metrics: List[str]
    ) -> Dict[str, Any]:
        """
        Fetch insights for a specific media.
        """
        VALID_METRICS = ["views", "likes", "replies", "reposts", "quotes", "shares"]
        invalid_metrics = [m for m in metrics if m not in VALID_METRICS]
        if invalid_metrics:
            return self._build_error(
                f"Invalid metric(s): {', '.join(invalid_metrics)}. Valid metrics: {', '.join(VALID_METRICS)}",
                code="INVALID_INPUT"
            )

        url = f"https://graph.threads.net/v1.0/{media_id}/insights"
        params = {"metric": ",".join(metrics)}
        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                return self._build_error(str(response.json()), code="MEDIA_INSIGHTS_FAILED")
        except requests.exceptions.RequestException as e:
            return self._build_error(f"Request failed: {e}", code="REQUEST_FAILED")

    # ---------------------
    # 5) Pretty Print JSON
    # ---------------------
    def pretty_print_json(self, data: Any) -> None:
        """
        Print JSON data in a human-readable format.
        """
        print(json.dumps(data, indent=4))

    # ---------------------
    # 6) Get List User Threads
    # ---------------------
    def get_list_user_threads(
        self,
        access_token: str,
        fields: List[str],
        since: Optional[int] = None,
        until: Optional[int] = None,
        limit: Optional[int] = None,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch user threads with support for field selection and pagination.
        """
        VALID_FIELDS = [
            "id", "media_product_type", "media_type", "media_url", "permalink", "owner",
            "username", "text", "timestamp", "shortcode", "thumbnail_url", "children",
            "is_quote_post", "quoted_post", "reposted_post", "has_replies", "alt_text",
            "link_attachment_url"
        ]
        invalid_fields = [f for f in fields if f not in VALID_FIELDS]
        if invalid_fields:
            return self._build_error(
                f"Invalid field(s): {', '.join(invalid_fields)}. Valid fields: {', '.join(VALID_FIELDS)}",
                code="INVALID_INPUT"
            )

        params = {
            "fields": ",".join(fields)
        }
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if limit:
            params["limit"] = limit
        if before:
            params["before"] = before
        elif after:
            params["after"] = after

        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            response = requests.get("https://graph.threads.net/me/threads", params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                return self._build_error(str(response.json()), code="THREADS_FETCH_FAILED")
        except requests.exceptions.RequestException as e:
            return self._build_error(f"Request failed: {e}", code="REQUEST_FAILED")

    # ---------------------
    # 7) Fetch All Threads with Pagination
    # ---------------------
    def fetch_all_threads_with_pagination(
        self,
        access_token: str,
        fields: List[str],
        since: Optional[int] = None,
        until: Optional[int] = None,
        limit: int = 50
    ) -> List[Any]:
        """
        Fetch all user threads with pagination using the 'after' cursor.
        """
        all_threads = []
        after_cursor = None

        while True:
            response = self.get_list_user_threads(
                access_token=access_token,
                fields=fields,
                since=since,
                until=until,
                limit=limit,
                after=after_cursor
            )

            if "error" in response:
                print(f"Error in fetch_all_threads_with_pagination: {response['error']}")
                break

            data_list = response.get("data", [])
            all_threads.extend(data_list)

            paging = response.get("paging", {})
            after_cursor = paging.get("cursors", {}).get("after")
            
            if not after_cursor:
                break

        return all_threads

    # ---------------------
    # 8) Convert Account Insights to DataFrame
    # ---------------------
    def convert_account_insights_to_dataframe(self, response: Dict[str, Any]) -> pd.DataFrame:
        """
        Convert the Threads API response into a pandas DataFrame, handle demographics, etc.
        """
        if "data" not in response:
            return pd.DataFrame()

        data = response["data"]
        rows = []

        since_val, until_val = None, None
        # Example: handle 'paging' to parse since/until from URLs
        if "paging" in response and "previous" in response["paging"]:
            prev_url = response["paging"]["previous"]
            since_match = re.search(r"since=(\d+)", prev_url)
            until_match = re.search(r"until=(\d+)", prev_url)
            if since_match:
                since_ts = int(since_match.group(1))
                since_val = datetime.fromtimestamp(since_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            if until_match:
                until_ts = int(until_match.group(1))
                until_val = datetime.fromtimestamp(until_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        for item in data:
            base_row = {
                "name": item.get("name"),
                "title": item.get("title"),
                "description": item.get("description"),
                "since": since_val,
                "until": until_val,
                "total_value": item.get("total_value", {}).get("value", None),
                "id": item.get("id"),
            }

            # Handle demographics
            if item.get("name") == "follower_demographics" and "total_value" in item:
                breakdowns = item["total_value"].get("breakdowns", [])
                for breakdown in breakdowns:
                    dimension_key = ",".join(breakdown.get("dimension_keys", []))
                    for result in breakdown.get("results", []):
                        row_copy = base_row.copy()
                        row_copy.update({
                            "dimension_key": dimension_key,
                            "dimension_value": result.get("dimension_values", [None])[0],
                            "value": result.get("value"),
                        })
                        rows.append(row_copy)
            elif item.get("name") == "views" and "values" in item:
                # Aggregate total views
                total_views = sum(v.get("value", 0) for v in item["values"])
                row_copy = base_row.copy()
                row_copy.update({
                    "total_value": total_views,
                    "value": None,
                    "end_time": None,
                })
                rows.append(row_copy)
            elif "values" in item:
                # Metrics with time-series values
                for value_item in item["values"]:
                    row_copy = base_row.copy()
                    row_copy.update({
                        "value": value_item.get("value"),
                        "end_time": value_item.get("end_time"),
                    })
                    rows.append(row_copy)
            else:
                rows.append(base_row)

        return pd.DataFrame(rows)

    # ---------------------
    # 9) Get Unix Time Frames
    # ---------------------
    def get_unix_time_frames(self) -> Dict[str, Dict[str, int]]:
        """
        Return multiple Unix timestamp ranges for time-based queries.
        """
        now = datetime.now(timezone.utc)

        # Current week (Monday - Sunday)
        current_week_start = now - timedelta(days=now.weekday())
        current_week_start = current_week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        current_week_end = current_week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)

        # Last week (Monday - Sunday)
        last_week_start = current_week_start - timedelta(days=7)
        last_week_end = last_week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)

        # Rolling 7 days
        rolling_7_days_start = now - timedelta(days=7)
        rolling_7_days_end = now

        # Rolling 90 days
        rolling_90_days_start = now - timedelta(days=90)
        rolling_90_days_end = now

        return {
            "last_week": {
                "start": int(last_week_start.timestamp()),
                "end": int(last_week_end.timestamp())
            },
            "current_week": {
                "start": int(current_week_start.timestamp()),
                "end": int(current_week_end.timestamp())
            },
            "rolling_7_days": {
                "start": int(rolling_7_days_start.timestamp()),
                "end": int(rolling_7_days_end.timestamp())
            },
            "rolling_90_days": {
                "start": int(rolling_90_days_start.timestamp()),
                "end": int(rolling_90_days_end.timestamp())
            }
        }

    # ---------------------
    # 10) Threads JSON to DataFrame
    # ---------------------
    def threads_json_to_dataframe(self, threads_json: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Converts a list of thread JSON objects into a Pandas DataFrame.
        """
        rows = []
        for thread in threads_json:
            row = {
                "id": thread.get("id"),
                "media_product_type": thread.get("media_product_type"),
                "media_type": thread.get("media_type"),
                "media_url": thread.get("media_url"),
                "permalink": thread.get("permalink"),
                "owner_id": thread.get("owner", {}).get("id"),
                "username": thread.get("username"),
                "text": thread.get("text"),
                "timestamp": thread.get("timestamp"),
                "shortcode": thread.get("shortcode"),
                "is_quote_post": thread.get("is_quote_post"),
                "has_replies": thread.get("has_replies"),
            }
            children = thread.get("children", {}).get("data", [])
            if children:
                row["children_ids"] = [child.get("id") for child in children]
            else:
                row["children_ids"] = None
            rows.append(row)

        print(f"Converted {len(rows)} threads to DataFrame.")
        return pd.DataFrame(rows)

    # ---------------------
    # 11) Fetch Insights for Media in DataFrame
    # ---------------------
    def fetch_insights_for_media_in_dataframe(
        self,
        access_token: str,
        dataframe: pd.DataFrame,
        metrics: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch insights for all media IDs in a given DataFrame.
        """
        insights_list = []
        for media_id in dataframe["id"]:
            resp = self.get_media_insights(access_token, media_id, metrics)
            if "error" in resp:
                print(f"Error fetching insights for media_id {media_id}: {resp['error']}")
                continue
            insights_list.append({"media_id": media_id, "insights": resp})
        
        print(f"Fetched insights for {len(insights_list)} media items.")
        return insights_list

    # ---------------------
    # 12) Insights to DataFrame
    # ---------------------
    def insights_to_dataframe(self, insights_list: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert the list of insights into a pandas DataFrame with metrics as columns.
        """
        rows = []
        for item in insights_list:
            media_id = item["media_id"]
            ins_data = item["insights"]
            if "data" in ins_data:
                row = {"media_id": media_id}
                for insight in ins_data["data"]:
                    metric_name = insight["name"]
                    if "values" in insight and len(insight["values"]) > 0:
                        metric_value = insight["values"][0].get("value")
                        row[metric_name] = metric_value
                rows.append(row)

        print(f"Converted insights for {len(rows)} media items to DataFrame.")
        return pd.DataFrame(rows)

    # ---------------------
    # 13) Fetch & Merge Threads with Insights
    # ---------------------
    def fetch_and_merge_threads_with_insights(
        self,
        access_token: str,
        fields: List[str],
        metrics: List[str],
        client_name: str,
        since: Optional[int] = None,
        until: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch threads, retrieve insights, and combine into a single DataFrame.
        """
        print("Fetching threads...")
        threads_json = self.fetch_all_threads_with_pagination(access_token, fields, since, until)
        
        if not threads_json:
            print("No threads found in the given time frame.")
            return pd.DataFrame()

        print("Converting threads JSON to DataFrame...")
        threads_df = self.threads_json_to_dataframe(threads_json)
        if threads_df.empty:
            print("No valid threads to process.")
            return threads_df

        print("Fetching insights for threads...")
        insights_list = self.fetch_insights_for_media_in_dataframe(access_token, threads_df, metrics)
        if not insights_list:
            print("No insights fetched for threads.")
            return threads_df

        print("Converting insights to DataFrame...")
        insights_df = self.insights_to_dataframe(insights_list)
        if insights_df.empty:
            print("No valid insights to merge.")
            return threads_df

        print("Merging threads with insights...")
        combined_df = pd.merge(threads_df, insights_df, left_on="id", right_on="media_id", how="left")
        combined_df.drop(columns=["media_id"], inplace=True)

        # Insert the 'client' and 'captured_at' columns
        captured_at_str = datetime.now(timezone.utc).isoformat()
        combined_df.insert(0, "client", client_name)
        combined_df.insert(1, "captured_at", captured_at_str)

        print(f"Final combined DataFrame has {len(combined_df)} rows.")
        return combined_df
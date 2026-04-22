import time

import googleapiclient.discovery
import googleapiclient.errors


class QuotaExhaustedError(Exception):
    """raised when all api keys have exceeded their quota."""

    pass


class YouTubeAPIManager:
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"

    def __init__(self, api_keys):
        if not api_keys:
            raise ValueError("No API keys provided.")

        self.api_keys = api_keys
        self.current_key_index = -1
        self._quota_failures_since_success = 0
        self.youtube = self._get_new_client()

    def _get_new_client(self):
        if self.current_key_index >= len(self.api_keys) - 1:
            if self._quota_failures_since_success >= len(self.api_keys):
                raise QuotaExhaustedError(
                    "all api keys have exceeded their quota. stopping."
                )
            self.current_key_index = 0
        else:
            self.current_key_index += 1

        developer_key = self.api_keys[self.current_key_index]
        print(f"using api key {self.current_key_index + 1}/{len(self.api_keys)}")
        return googleapiclient.discovery.build(
            self.YOUTUBE_API_SERVICE_NAME,
            self.YOUTUBE_API_VERSION,
            developerKey=developer_key,
        )

    def make_request(self, method_func, **kwargs):
        while True:
            try:
                request = method_func(self.youtube, **kwargs)
                result = request.execute()
                self._quota_failures_since_success = 0
                return result

            except googleapiclient.errors.HttpError as e:
                reason = e.error_details[0]["reason"] if e.error_details else "unknown"

                if reason == "quotaExceeded":
                    print("api key quota exceeded. rotating to next key...")
                    self._quota_failures_since_success += 1
                    self.youtube = self._get_new_client()
                else:
                    raise

            except Exception as e:
                print(f"connection error: {e}. retrying in 5s...")
                time.sleep(5)

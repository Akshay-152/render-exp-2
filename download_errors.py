"""Custom exception hierarchy for the YouTube downloader.

These exceptions carry structured context (original yt-dlp message, video URL,
and video ID) so that callers can log actionable, consistent errors instead of
relying on opaque string matching at every call site.
"""

from __future__ import annotations

from typing import Optional


def extract_video_id(url: str) -> Optional[str]:
    """Best-effort extraction of a YouTube video ID from a URL.

    Handles common forms:
      - https://www.youtube.com/watch?v=VIDEO_ID
      - https://youtu.be/VIDEO_ID
      - https://www.youtube.com/shorts/VIDEO_ID
      - https://www.youtube.com/embed/VIDEO_ID

    Returns the video ID, or None if one cannot be determined.
    """
    if not url:
        return None
    # "watch?v=" query param
    marker = "watch?v="
    if marker in url:
        segment = url.split(marker, 1)[1]
        return segment.split("&", 1)[0] or None
    # youtu.be/<id>
    if "youtu.be/" in url:
        segment = url.split("youtu.be/", 1)[1]
        return segment.split("?")[0].split("/")[0] or None
    # /shorts/<id> or /embed/<id>
    for kw in ("/short/", "/shorts/", "/embed/"):
        if kw in url:
            segment = url.split(kw, 1)[1]
            return segment.split("?")[0].split("/")[0] or None
    return None


class DownloadError(Exception):
    """Base class for all downloader-specific errors.

    Attributes:
        original_message: The raw message from yt-dlp, if any.
        url: The video URL that failed.
        video_id: The extracted YouTube video ID, if available.
    """

    def __init__(
        self,
        message: str,
        *,
        url: str = "",
        video_id: Optional[str] = None,
        original_message: str = "",
    ) -> None:
        self.original_message = original_message
        self.url = url
        self.video_id = video_id or extract_video_id(url)
        super().__init__(self._format(message))

    def _format(self, message: str) -> str:
        """Attach URL / video ID context to the human-readable message."""
        parts = [message]
        if self.url:
            parts.append(f"URL: {self.url}")
        if self.video_id:
            parts.append(f"Video ID: {self.video_id}")
        if self.original_message:
            parts.append(f"Original yt-dlp error: {self.original_message}")
        return " | ".join(parts)


class AuthenticationRequiredError(DownloadError):
    """YouTube requires authentication to process this video.

    This is a *permanent* condition — retrying the same request will never
    succeed. Callers should stop and inform the user to supply cookies.
    """


class RetryableDownloadError(DownloadError):
    """A transient failure (network timeout, 5xx, connection reset).

    Retrying with exponential backoff may succeed.
    """


class VideoUnavailableError(DownloadError):
    """The video is private, removed, unavailable, or geoblocked.

    This is permanent — the video cannot be downloaded regardless of retries.
    """


class MetadataError(DownloadError):
    """Failed to fetch metadata for the video.

    This may be retryable or permanent; the exception type is determined by
    the underlying error classification.
    """

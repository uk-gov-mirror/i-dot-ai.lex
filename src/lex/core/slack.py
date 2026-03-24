"""Lightweight Slack webhook notifications for job status updates.

Uses Slack Block Kit for polished messages. Reads SLACK_WEBHOOK_URL from
environment. All functions return False silently if the URL is unset —
notifications must never crash a job.
"""

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def _format_duration(seconds: int | None) -> str:
    """Format seconds into human-readable duration."""
    if seconds is None:
        return ""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _timestamp_context() -> dict:
    """Build a context block with the current UTC time."""
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": now}],
    }


def _send(text: str, blocks: list[dict]) -> bool:
    """Send a message to Slack via webhook. Returns True on success."""
    if not SLACK_WEBHOOK_URL:
        logger.debug("SLACK_WEBHOOK_URL not set, skipping notification")
        return False

    payload = {"text": text, "blocks": blocks}

    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")
        return False


def notify_job_start(job_name: str, details: dict | None = None) -> bool:
    """Notify that a job has started."""
    text = f":rocket: {job_name} — Started"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f":rocket:  {job_name}", "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Started*"},
        },
    ]

    if details:
        fields = [
            {"type": "mrkdwn", "text": f"*{k}:*  {v}"}
            for k, v in details.items()
        ]
        blocks.append({"type": "section", "fields": fields})

    blocks.append({"type": "divider"})
    blocks.append(_timestamp_context())

    return _send(text, blocks)


def notify_job_success(
    job_name: str, stats: dict, duration_seconds: int | None = None
) -> bool:
    """Notify that a job completed successfully with summary stats."""
    duration = _format_duration(duration_seconds)
    subtitle = f"Completed in {duration}" if duration else "Completed"
    text = f":white_check_mark: {job_name} — {subtitle}"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":white_check_mark:  {job_name}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{subtitle}*"},
        },
    ]

    if stats:
        fields = [
            {"type": "mrkdwn", "text": f"*{k}:*  `{v}`"}
            for k, v in stats.items()
        ]
        # Slack limits fields to 10 per section
        for i in range(0, len(fields), 10):
            blocks.append({"type": "section", "fields": fields[i : i + 10]})

    blocks.append({"type": "divider"})
    blocks.append(_timestamp_context())

    return _send(text, blocks)


def notify_job_failure(
    job_name: str, error: str, duration_seconds: int | None = None
) -> bool:
    """Notify that a job failed."""
    duration = _format_duration(duration_seconds)
    subtitle = f"Failed after {duration}" if duration else "Failed"
    text = f":x: {job_name} — {subtitle}"

    # Truncate long errors for Slack (3000 char limit per text block)
    error_display = error[:2500] if len(error) > 2500 else error

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f":x:  {job_name}", "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{subtitle}*"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{error_display}```"},
        },
        {"type": "divider"},
        _timestamp_context(),
    ]

    return _send(text, blocks)

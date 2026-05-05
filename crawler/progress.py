import json
import os
import shutil
import sys
from datetime import datetime


class VideoProgress:
    """
    in-place progress display for video comment collection.
    renders a single line that updates via \\r (carriage return).
    falls back to per-video-only output when stdout is not a TTY.
    """

    def __init__(self, youtuber, output_dir):
        self.youtuber = youtuber
        self.output_dir = output_dir
        self.video_id = ""
        self.video_title = ""
        self.video_comments = 0
        self.video_replies = 0
        self.total = 0
        self.videos_done = 0
        self._is_tty = sys.stdout.isatty()
        self._last_page = 0

    def set_video(self, video_id, video_title):
        self.video_id = video_id
        self.video_title = video_title
        self.video_comments = 0
        self.video_replies = 0
        self._last_page = 0
        if self._is_tty:
            self._redraw()

    def add_comments(self, count):
        self.video_comments += count
        self.total += count
        self._maybe_redraw()

    def add_replies(self, count):
        self.video_replies += count
        self.total += count
        self._maybe_redraw()

    def video_done(self):
        self.videos_done += 1
        video_total = self.video_comments + self.video_replies
        msg = (
            f"  \u2713 {self.video_id} \"{self.video_title[:60]}\""
            f" | {self.video_comments}c + {self.video_replies}r = {video_total}"
            f" | Total: {self.total}"
        )
        if self._is_tty:
            self._clear_line()
        print(msg)
        self._log_line(msg)
        self.video_id = ""
        self.video_title = ""
        self.video_comments = 0
        self.video_replies = 0

    def log_header(self, limit):
        header = f"Collecting the first {limit} videos (if not collected already)"
        self._log_line(header)

    def _log_path(self):
        return os.path.join(self.output_dir, "crawl.log")

    def _log_line(self, line):
        with open(self._log_path(), "a") as f:
            f.write(line + "\n")

    def save_log(self, collected_ids=None, label=""):
        video_total = self.video_comments + self.video_replies
        ids = ", ".join(collected_ids[-20:]) if collected_ids else "—"
        marker = {"partial": "  partial", "final": "  final", "error": "  error", "quota_exhausted": "  quota exhausted"}.get(label, f"  {label}")
        lines = [
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]{marker}",
            f"  @{self.youtuber} | {self.video_id} \"{self.video_title[:60]}\"",
            f"  Video: {self.video_comments}c + {self.video_replies}r = {video_total} | Total: {self.total} | Videos done: {self.videos_done}",
            f"  Collected: {ids}",
            "",
        ]
        with open(self._log_path(), "a") as f:
            f.write("\n".join(lines))

    def _maybe_redraw(self):
        if self._is_tty:
            self._redraw()
        else:
            page = self.video_comments // 100
            if page > self._last_page:
                self._last_page = page
                video_total = self.video_comments + self.video_replies
                print(
                    f"  @{self.youtuber} | {self.video_id}"
                    f" | {self.video_comments}c + {self.video_replies}r = {video_total}"
                    f" | Total: {self.total}"
                )

    def _redraw(self):
        self._clear_line()
        video_total = self.video_comments + self.video_replies
        title = (
            self.video_title[:50] + "..."
            if len(self.video_title) > 50
            else self.video_title
        )
        status = (
            f"@{self.youtuber} | {self.video_id} \"{title}\""
            f" | Video: {self.video_comments}c + {self.video_replies}r = {video_total}"
            f" | Total: {self.total}"
        )
        width = shutil.get_terminal_size().columns
        if len(status) > width:
            status = status[: width - 1]
        sys.stdout.write(status)
        sys.stdout.flush()

    def _clear_line(self):
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

import logging
import time
from collections.abc import Generator, Iterator
from typing import Any

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from github_ingest.config import Settings

logger = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    if not isinstance(exc, requests.HTTPError):
        return False
    status = exc.response.status_code if exc.response is not None else 0
    if status >= 500:
        return True
    return status in (403, 429)


class PagedResult:
    """Iterable wrapper around a paginated GitHub response sequence.

    After iteration, ``etag`` and ``last_modified`` reflect the headers of the
    first-page response (304 → was_304=True, both are None).
    """

    def __init__(
        self,
        gen: Generator[dict[str, Any], None, None],
        etag: str | None,
        last_modified: str | None,
        was_304: bool,
    ) -> None:
        self._gen = gen
        self.etag = etag
        self.last_modified = last_modified
        self.was_304 = was_304

    def __iter__(self) -> Iterator[dict[str, Any]]:
        yield from self._gen


class GitHubClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session = requests.Session()
        headers: dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if settings.github_token is not None:
            headers["Authorization"] = f"Bearer {settings.github_token.get_secret_value()}"
        else:
            logger.warning("No GITHUB_TOKEN set — rate limit is 60 req/hr")
        self._session.headers.update(headers)

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> requests.Response:
        url = f"{self._settings.github_base_url}{path}"
        extra_headers: dict[str, str] = {}
        if etag:
            extra_headers["If-None-Match"] = etag
        if last_modified:
            extra_headers["If-Modified-Since"] = last_modified

        logger.debug("Request issued", extra={"endpoint": path})

        @retry(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(self._settings.max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=60),
            reraise=True,
        )
        def _do_get() -> requests.Response:
            resp = self._session.get(
                url,
                params=params,
                headers=extra_headers,
                timeout=self._settings.request_timeout,
            )
            if resp.status_code == 304:
                return resp
            self._handle_rate_limit(resp)
            resp.raise_for_status()
            return resp

        return _do_get()

    def _handle_rate_limit(self, resp: requests.Response) -> None:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None and int(remaining) == 0 and reset is not None:
            sleep_for = max(0, int(reset) - int(time.time())) + 1
            logger.warning(
                "Rate limit exhausted, sleeping %ss",
                sleep_for,
                extra={"sleep_seconds": sleep_for},
            )
            time.sleep(sleep_for)

        if resp.status_code in (403, 429):
            retry_after = resp.headers.get("Retry-After")
            if retry_after is not None:
                logger.warning(
                    "Secondary rate limit, sleeping %ss",
                    retry_after,
                    extra={"sleep_seconds": int(retry_after)},
                )
                time.sleep(int(retry_after))
            resp.raise_for_status()

    def paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> "PagedResult":
        first_resp = self._fetch_first_page(path, params, etag, last_modified)

        if first_resp.status_code == 304:
            return PagedResult(_empty_gen(), etag, last_modified, was_304=True)

        first_etag = first_resp.headers.get("ETag")
        first_last_modified = first_resp.headers.get("Last-Modified")

        return PagedResult(
            self._paginate_gen(first_resp, params),
            etag=first_etag,
            last_modified=first_last_modified,
            was_304=False,
        )

    def _fetch_first_page(
        self,
        path: str,
        params: dict[str, Any] | None,
        etag: str | None,
        last_modified: str | None,
    ) -> requests.Response:
        merged: dict[str, Any] = dict(params or {})
        merged.setdefault("per_page", self._settings.page_size)
        return self.get(path, params=merged, etag=etag, last_modified=last_modified)

    def _paginate_gen(
        self,
        first_resp: requests.Response,
        params: dict[str, Any] | None,
    ) -> Generator[dict[str, Any], None, None]:
        resp = first_resp
        while True:
            data = resp.json()
            if isinstance(data, list):
                yield from data
            else:
                yield data
                break

            next_url = _parse_next_link(resp.headers.get("Link", ""))
            if next_url is None:
                break

            if next_url.startswith(self._settings.github_base_url):
                rel_path = next_url[len(self._settings.github_base_url) :]
                resp = self.get(rel_path)
            else:
                raw_resp = self._session.get(next_url, timeout=self._settings.request_timeout)
                raw_resp.raise_for_status()
                resp = raw_resp

    def close(self) -> None:
        self._session.close()


def _empty_gen() -> Generator[dict[str, Any], None, None]:
    return
    yield  # makes it a generator


def _parse_next_link(link_header: str) -> str | None:
    for part in link_header.split(","):
        segments = part.strip().split(";")
        if len(segments) == 2:
            url_part = segments[0].strip().strip("<>")
            rel_part = segments[1].strip()
            if rel_part == 'rel="next"':
                return url_part
    return None

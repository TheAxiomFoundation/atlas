"""Batched Supabase upsert for rules."""
import os
import time
from typing import Iterable
import httpx

_TIMEOUT = httpx.Timeout(180.0, connect=30.0, read=180.0, write=180.0)


class RuleUploader:
    def __init__(self, url: str | None = None, key: str | None = None):
        self.url = url or os.environ.get(
            "COSILICO_SUPABASE_URL",
            "https://nsupqhfchdtqclomlrgs.supabase.co",
        )
        self.key = key or self._get_service_key()
        self.rest_url = f"{self.url}/rest/v1"

    def _get_service_key(self) -> str:
        """Get service role key from Supabase Management API."""
        access_token = os.environ.get("SUPABASE_ACCESS_TOKEN")
        if not access_token:
            raise ValueError(
                "SUPABASE_ACCESS_TOKEN env var required to get service key"
            )
        project_ref = self.url.split("//")[1].split(".")[0]
        with httpx.Client() as client:
            response = client.get(
                f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            response.raise_for_status()
            for key in response.json():
                if key.get("name") == "service_role" and key.get("api_key"):
                    return key["api_key"]
        raise ValueError("Could not find service_role key")

    def upsert_batch(
        self,
        rules: list[dict],
        max_retries: int = 5,
        client: httpx.Client | None = None,
    ) -> int:
        """Upsert a batch of rules. Adds line_count, retries on timeout/5xx."""
        if not rules:
            return 0

        for rule in rules:
            body = rule.get("body") or ""
            rule["line_count"] = len(body.split("\n"))

        def _do_post(c: httpx.Client) -> int:
            for attempt in range(max_retries):
                try:
                    response = c.post(
                        f"{self.rest_url}/rules",
                        headers={
                            "apikey": self.key,
                            "Authorization": f"Bearer {self.key}",
                            "Content-Type": "application/json",
                            "Content-Profile": "arch",
                            "Prefer": "resolution=ignore-duplicates,return=minimal",
                        },
                        json=rules,
                    )
                    response.raise_for_status()
                    return len(rules)
                except (httpx.ReadTimeout, httpx.HTTPStatusError) as e:
                    is_server_error = (
                        isinstance(e, httpx.HTTPStatusError)
                        and e.response.status_code >= 500
                    )
                    is_timeout = isinstance(e, httpx.ReadTimeout)

                    if (is_server_error or is_timeout) and attempt < max_retries - 1:
                        time.sleep(2**attempt)
                        continue
                    raise
            return len(rules)

        if client:
            return _do_post(client)
        with httpx.Client(timeout=_TIMEOUT) as c:
            return _do_post(c)

    def upsert_all(
        self, rules: list[dict] | Iterable[dict], batch_size: int = 50
    ) -> int:
        """Deduplicate by citation_path then batch upsert."""
        # Deduplicate in single pass: last wins
        seen: dict[str, dict] = {}
        for rule in rules:
            path = rule.get("citation_path", "")
            if path:
                seen[path] = rule
            else:
                seen[str(id(rule))] = rule

        if not seen:
            return 0

        unique = list(seen.values())
        total = 0
        with httpx.Client(timeout=_TIMEOUT) as client:
            for i in range(0, len(unique), batch_size):
                batch = unique[i : i + batch_size]
                total += self.upsert_batch(batch, client=client)
        return total

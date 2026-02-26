from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class CheckoutResult:
    ok: bool
    url: str
    session_id: str
    detail: str


class StripeCheckoutClient:
    def __init__(self, secret_key: str) -> None:
        self.secret_key = secret_key

    def create_checkout_session(
        self,
        *,
        amount_value: int,
        currency: str,
        lead_id: int,
        plan: str,
        business_name: str,
        success_url: str,
        cancel_url: str,
    ) -> CheckoutResult:
        amount_cents = max(100, int(amount_value) * 100)
        currency_code = (currency or "brl").strip().lower()
        payload = {
            "mode": "payment",
            "success_url": success_url,
            "cancel_url": cancel_url,
            "line_items[0][price_data][currency]": currency_code,
            "line_items[0][price_data][unit_amount]": str(amount_cents),
            "line_items[0][price_data][product_data][name]": f"Site {plan} - {business_name}",
            "line_items[0][quantity]": "1",
            "metadata[lead_id]": str(lead_id),
            "metadata[plan]": str(plan),
            "metadata[amount]": str(amount_value),
            "metadata[currency]": currency_code,
        }
        req = Request(
            "https://api.stripe.com/v1/checkout/sessions",
            data=urlencode(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.secret_key}",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "leadgen-stripe/1.0",
            },
        )
        try:
            with urlopen(req, timeout=25) as res:
                body = json.loads(res.read().decode("utf-8"))
            return CheckoutResult(ok=True, url=str(body.get("url", "")), session_id=str(body.get("id", "")), detail="")
        except HTTPError as exc:
            raw = ""
            try:
                raw = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                raw = ""
            return CheckoutResult(ok=False, url="", session_id="", detail=f"http_{exc.code}:{raw[:180]}")
        except URLError as exc:
            return CheckoutResult(ok=False, url="", session_id="", detail=f"network:{exc.reason}")

    def retrieve_session(self, session_id: str) -> dict:
        req = Request(
            f"https://api.stripe.com/v1/checkout/sessions/{session_id}",
            method="GET",
            headers={
                "Authorization": f"Bearer {self.secret_key}",
                "User-Agent": "leadgen-stripe/1.0",
            },
        )
        with urlopen(req, timeout=25) as res:
            return json.loads(res.read().decode("utf-8"))


def get_stripe_client_from_env() -> StripeCheckoutClient | None:
    key = os.getenv("STRIPE_SECRET_KEY", "").strip()
    if not key:
        return None
    return StripeCheckoutClient(key)

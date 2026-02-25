from __future__ import annotations

import json
import os
import random
import re
import time
import uuid
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class DeliveryResult:
    ok: bool
    message_id: str
    status: str
    detail: str


class ResendEmailClient:
    def __init__(self, api_key: str, from_email: str) -> None:
        self.api_key = api_key
        self.from_email = from_email

    def send(self, to_email: str, subject: str, html: str) -> DeliveryResult:
        body = {
            "from": self.from_email,
            "to": [to_email],
            "subject": subject,
            "html": html,
        }
        req = Request(
            "https://api.resend.com/emails",
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "leadgen-outreach/1.0",
            },
        )
        try:
            with urlopen(req, timeout=25) as res:
                payload = json.loads(res.read().decode("utf-8"))
            return DeliveryResult(ok=True, message_id=str(payload.get("id", "")), status="sent", detail="")
        except HTTPError as exc:
            return DeliveryResult(ok=False, message_id="", status="http_error", detail=f"{exc.code}")
        except URLError as exc:
            return DeliveryResult(ok=False, message_id="", status="network_error", detail=str(exc.reason))


class WppConnectClient:
    def __init__(self, base_url: str, token: str, instance: str, send_path: str = "/api/{instance}/send-message") -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.instance = instance
        self.send_path = send_path

    def send(self, phone_e164: str, text: str) -> DeliveryResult:
        url = f"{self.base_url}{self.send_path.format(instance=self.instance)}"
        payload = {"phone": phone_e164, "message": text}
        req = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(req, timeout=25) as res:
                body = json.loads(res.read().decode("utf-8", errors="ignore") or "{}")
            message_id = str(body.get("id") or body.get("messageId") or uuid.uuid4())
            return DeliveryResult(ok=True, message_id=message_id, status="sent", detail="")
        except HTTPError as exc:
            return DeliveryResult(ok=False, message_id="", status="http_error", detail=f"{exc.code}")
        except URLError as exc:
            return DeliveryResult(ok=False, message_id="", status="network_error", detail=str(exc.reason))


def normalize_phone_br(raw: str) -> str:
    digits = re.sub(r"\D+", "", raw or "")
    if not digits:
        return ""
    if digits.startswith("55"):
        return digits
    if len(digits) >= 10:
        return f"55{digits}"
    return ""


def random_human_delay(min_seconds: float = 1.8, max_seconds: float = 4.2) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


def build_unsubscribe_url(base_url: str, lead_id: int, channel: str) -> str:
    query = urlencode({"lead_id": lead_id, "channel": channel})
    return f"{base_url.rstrip('/')}/unsubscribe?{query}"


def initial_consent_email(name: str, unsubscribe_url: str, variant: int = 1) -> tuple[str, str, str]:
    subject_options = [
        f"{name}: posso te mostrar uma demo gratis?",
        f"Ideia rapida para {name}",
        f"Sugestao curta para aumentar contatos no Google",
    ]
    subjects = subject_options[min(max(variant, 1), 3) - 1]
    body = (
        f"Oi, equipe {name}.\\n\\n"
        "Vi o perfil de voces no Google e pensei em montar uma demo gratis de pagina para captar mais contatos.\\n"
        "Se fizer sentido, eu te mando sem custo e sem compromisso.\\n\\n"
        "Se nao quiser mais mensagens, use este link: "
        f"{unsubscribe_url}"
    )
    html = body.replace("\n", "<br>")
    return subjects, body, html


def initial_consent_whatsapp(name: str) -> str:
    return (
        f"Oi, equipe {name}. Vi o perfil de voces no Google e posso montar uma demo gratis de pagina para captar mais contatos. "
        "Posso te enviar? Se nao quiser receber mensagens, responda PARAR."
    )


def followup_consent_email(name: str, unsubscribe_url: str, step: int) -> tuple[str, str, str]:
    subject = f"{name}: continuo com a demo gratis?"
    body = (
        f"Oi, equipe {name}.\\n\\n"
        "Passando para confirmar se querem que eu monte a demo gratis da pagina. "
        "Se nao fizer sentido agora, sem problema.\\n\\n"
        "Para parar as mensagens: "
        f"{unsubscribe_url}"
    )
    if step >= 2:
        body = (
            f"Oi, equipe {name}. Este e meu ultimo contato sobre a demo gratis.\\n\\n"
            "Se quiserem, eu preparo e envio. Se nao, encerro por aqui.\\n\\n"
            "Opt-out: "
            f"{unsubscribe_url}"
        )
    return subject, body, body.replace("\\n", "<br>")


def followup_consent_whatsapp(name: str, step: int) -> str:
    if step >= 2:
        return (
            f"{name}, ultimo toque por aqui sobre a demo gratis. "
            "Se quiser receber, responda SIM. Para sair, responda PARAR."
        )
    return (
        f"{name}, posso seguir com a demo gratis da sua pagina? "
        "Se nao quiser mais mensagens, responda PARAR."
    )


def offer_email(name: str, preview_url: str, payment_url: str, unsubscribe_url: str) -> tuple[str, str, str]:
    subject = f"{name}: demo pronta + proposta"
    body = (
        f"Perfeito, equipe {name}.\\n\\n"
        f"Sua demo esta pronta: {preview_url}\\n"
        "Se curtir, eu finalizo e publico com configuracao completa.\\n"
        f"Pagamento: {payment_url}\\n\\n"
        "Se nao quiser mais mensagens, use: "
        f"{unsubscribe_url}"
    )
    html = body.replace("\n", "<br>")
    return subject, body, html


def offer_whatsapp(name: str, preview_url: str, payment_url: str) -> str:
    return (
        f"{name}, sua demo ficou pronta: {preview_url}. "
        f"Se quiser que eu publique a versao final hoje, segue pagamento: {payment_url}. "
        "Se quiser parar as mensagens, responda PARAR."
    )


def is_positive_reply(text: str) -> bool:
    t = (text or "").lower()
    positives = ["sim", "pode", "manda", "quero", "ok", "pode enviar", "tenho interesse"]
    return any(p in t for p in positives)


def is_opt_out_reply(text: str) -> bool:
    t = (text or "").strip().lower()
    return t in {"parar", "sair", "stop", "unsubscribe", "cancelar"}


def classify_reply(text: str) -> tuple[str, float]:
    t = (text or "").lower()
    if is_opt_out_reply(t):
        return "opt_out", 0.99
    if is_positive_reply(t):
        return "positive", 0.85
    if "preco" in t or "caro" in t:
        return "objection_price", 0.8
    if "depois" in t or "agora nao" in t:
        return "not_now", 0.8
    if "confi" in t or "garantia" in t:
        return "objection_trust", 0.75
    return "neutral", 0.5


def get_resend_client_from_env() -> ResendEmailClient | None:
    key = os.getenv("RESEND_API_KEY", "").strip()
    from_email = os.getenv("RESEND_FROM_EMAIL", "").strip()
    if not key or not from_email:
        return None
    return ResendEmailClient(api_key=key, from_email=from_email)


def get_wpp_client_from_env() -> WppConnectClient | None:
    base_url = os.getenv("WPP_BASE_URL", "").strip()
    token = os.getenv("WPP_TOKEN", "").strip()
    instance = os.getenv("WPP_INSTANCE", "default").strip() or "default"
    send_path = os.getenv("WPP_SEND_PATH", "/api/{instance}/send-message").strip()
    if not base_url or not token:
        return None
    return WppConnectClient(base_url=base_url, token=token, instance=instance, send_path=send_path)

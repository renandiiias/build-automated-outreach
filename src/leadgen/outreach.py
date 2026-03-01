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
    if digits.startswith("00"):
        digits = digits[2:]
    if raw.strip().startswith("+") and 8 <= len(digits) <= 15:
        return digits
    if 8 <= len(digits) <= 15 and not digits.startswith("0"):
        return digits
    default_cc = re.sub(r"\D+", "", os.getenv("LEADGEN_DEFAULT_COUNTRY_CODE", "55")) or "55"
    if digits.startswith("0"):
        digits = digits.lstrip("0")
    if len(digits) >= 6:
        return f"{default_cc}{digits}"
    return ""


def random_human_delay(min_seconds: float = 1.8, max_seconds: float = 4.2) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


def build_unsubscribe_url(base_url: str, lead_id: int, channel: str) -> str:
    query = urlencode({"lead_id": lead_id, "channel": channel})
    return f"{base_url.rstrip('/')}/unsubscribe?{query}"


def _is_pt_br(locale: str) -> bool:
    return (locale or "").strip().lower().startswith("pt")


def _is_spanish(locale: str) -> bool:
    return (locale or "").strip().lower().startswith("es")


def _location_hint(value: str, locale: str) -> str:
    raw = (value or "").strip()
    if not raw:
        if _is_pt_br(locale):
            return "sua cidade"
        if _is_spanish(locale):
            return "tu ciudad"
        return "your city"
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        return raw
    country_words = {
        "brazil", "brasil", "portugal", "united kingdom", "england", "usa", "united states", "spain", "espana", "españa"
    }
    filtered = [p for p in parts if p.lower() not in country_words]
    parts = filtered or parts
    if len(parts) >= 3:
        candidate = parts[-2] if parts[-1].lower() in {"faro", "lisboa", "lisbon", "porto", "madrid", "catalunya", "barcelona"} else parts[-1]
        return candidate
    if len(parts) == 2:
        candidate = parts[-1]
        return candidate
    return parts[0]


def _identity_service_label(service_hint: str, locale: str) -> str:
    raw = (service_hint or "").strip().lower()
    if any(tok in raw for tok in ["lawyer", "advogado", "abogado", "attorney", "solicitor"]):
        if _is_pt_br(locale):
            return "advogados"
        if _is_spanish(locale):
            return "abogados"
        return "law firms"
    if any(tok in raw for tok in ["accountant", "contador", "contable", "accounting"]):
        if _is_pt_br(locale):
            return "contadores"
        if _is_spanish(locale):
            return "contables"
        return "accountants"
    if any(tok in raw for tok in ["dentist", "dentista", "dental", "odont"]):
        if _is_pt_br(locale):
            return "dentistas"
        if _is_spanish(locale):
            return "dentistas"
        return "dentists"
    if any(tok in raw for tok in ["physiotherapist", "fisioterapeuta", "fisioterapia", "physio"]):
        if _is_pt_br(locale):
            return "fisioterapeutas"
        if _is_spanish(locale):
            return "fisioterapeutas"
        return "physiotherapists"
    if any(tok in raw for tok in ["psychologist", "psicologo", "psicólogo", "therapist", "therapy"]):
        if _is_pt_br(locale):
            return "psicologos"
        if _is_spanish(locale):
            return "psicologos"
        return "psychologists"
    if any(tok in raw for tok in ["architect", "arquiteto", "arquitecto", "architecture"]):
        if _is_pt_br(locale):
            return "arquitetos"
        if _is_spanish(locale):
            return "arquitectos"
        return "architects"
    if any(tok in raw for tok in ["financial advisor", "consultor financeiro", "asesor financiero"]):
        if _is_pt_br(locale):
            return "consultores financeiros"
        if _is_spanish(locale):
            return "asesores financieros"
        return "financial advisors"
    if any(tok in raw for tok in ["insurance broker", "corretor de seguros", "corredor de seguros"]):
        if _is_pt_br(locale):
            return "corretores de seguros"
        if _is_spanish(locale):
            return "corredores de seguros"
        return "insurance brokers"
    if any(tok in raw for tok in ["immigration consultant", "consultor de imigracao", "consultor de inmigracion"]):
        if _is_pt_br(locale):
            return "consultores de imigracao"
        if _is_spanish(locale):
            return "consultores de inmigracion"
        return "immigration consultants"
    if any(tok in raw for tok in ["business consultant", "consultor empresarial", "consultor de negocios"]):
        if _is_pt_br(locale):
            return "consultores empresariais"
        if _is_spanish(locale):
            return "consultores de negocios"
        return "business consultants"
    if "hotel" in raw:
        if _is_pt_br(locale):
            return "hoteis"
        if _is_spanish(locale):
            return "hoteles"
        return "hotels"
    if "cowork" in raw:
        if _is_pt_br(locale):
            return "espacos de coworking"
        if _is_spanish(locale):
            return "espacios de coworking"
        return "coworking spaces"
    if "electrician" in raw or "eletricista" in raw or "electricista" in raw:
        if _is_pt_br(locale):
            return "eletricistas"
        if _is_spanish(locale):
            return "electricistas"
        return "electricians"
    if "plumber" in raw or "encanador" in raw or "fontanero" in raw:
        if _is_pt_br(locale):
            return "encanadores"
        if _is_spanish(locale):
            return "fontaneros"
        return "plumbers"
    if _is_pt_br(locale):
        return "negocios como o de voces"
    if _is_spanish(locale):
        return "negocios como el de ustedes"
    return "businesses like yours"


def _service_category(service_hint: str) -> str:
    raw = (service_hint or "").strip().lower()
    if any(tok in raw for tok in ["lawyer", "advogado", "abogado", "attorney", "solicitor"]):
        return "lawyer"
    if any(tok in raw for tok in ["accountant", "contador", "contable", "accounting"]):
        return "accountant"
    if any(tok in raw for tok in ["dentist", "dentista", "dental", "odont"]):
        return "dentist"
    if any(tok in raw for tok in ["physiotherapist", "fisioterapeuta", "fisioterapia", "physio"]):
        return "physio"
    if any(tok in raw for tok in ["psychologist", "psicologo", "psicólogo", "therapist", "therapy"]):
        return "psychologist"
    if any(tok in raw for tok in ["architect", "arquiteto", "arquitecto", "architecture"]):
        return "architect"
    if any(tok in raw for tok in ["financial advisor", "consultor financeiro", "asesor financiero"]):
        return "financial_advisor"
    if any(tok in raw for tok in ["insurance broker", "corretor de seguros", "corredor de seguros"]):
        return "insurance_broker"
    if any(tok in raw for tok in ["immigration consultant", "consultor de imigracao", "consultor de inmigracion"]):
        return "immigration_consultant"
    if any(tok in raw for tok in ["business consultant", "consultor empresarial", "consultor de negocios"]):
        return "business_consultant"
    if any(tok in raw for tok in ["electrician", "eletricista", "electricista"]):
        return "electrician"
    if any(tok in raw for tok in ["plumber", "encanador", "fontanero"]):
        return "plumber"
    if any(tok in raw for tok in ["locksmith", "chaveiro", "cerrajero"]):
        return "locksmith"
    if any(tok in raw for tok in ["ac", "ar condicionado", "air conditioning", "hvac"]):
        return "hvac"
    return "general_service"


def _pt_service_pitch(category: str, ab_variant: str, has_website: bool) -> str:
    if has_website:
        if ab_variant == "B":
            return "E consigo melhorar bem o visual e os blocos de conversao do site atual para gerar mais pedidos."
        return "E consigo elevar bastante o visual e a clareza do site atual para transformar visitas em pedidos."
    mapping_a = {
        "lawyer": "Focada para advocacia, com linguagem de confianca e CTA direto para consulta.",
        "accountant": "Focada para contabilidade, com oferta clara de diagnostico e CTA para atendimento rapido.",
        "dentist": "Focada para dentista, com destaque para procedimentos e CTA para agendamento.",
        "physio": "Focada para fisioterapia, com destaque para tratamentos e CTA para avaliacao inicial.",
        "psychologist": "Focada para psicologia, com abordagem acolhedora e CTA para primeira sessao.",
        "architect": "Focada para arquitetura, com portfolio visual e CTA para briefing inicial.",
        "financial_advisor": "Focada para consultoria financeira, com posicionamento premium e CTA para analise inicial.",
        "insurance_broker": "Focada para corretor de seguros, com comparativo de opcoes e CTA para cotacao.",
        "immigration_consultant": "Focada para consultoria de imigracao, com passos claros e CTA para avaliacao do caso.",
        "business_consultant": "Focada para consultoria empresarial, com proposta de valor objetiva e CTA para reuniao.",
        "electrician": "Focada em eletricista, com CTA direto para urgencia, instalacao e manutencao.",
        "plumber": "Focada em encanador, com CTA direto para vazamento, desentupimento e reparo rapido.",
        "locksmith": "Focada em chaveiro, com CTA direto para emergencia, abertura e troca de fechadura.",
        "hvac": "Focada em climatizacao, com CTA direto para instalacao, limpeza e manutencao.",
        "general_service": "Focada em servico local, com CTA direto para orcamento e atendimento rapido.",
    }
    mapping_b = {
        "lawyer": "Com estrutura para advocacia: prova social, areas atendidas e botao de contato imediato.",
        "accountant": "Com estrutura para contabilidade: servicos principais, prova local e botao de contato imediato.",
        "dentist": "Com estrutura para dentista: especialidades, prova local e botao de agendamento imediato.",
        "physio": "Com estrutura para fisioterapia: tratamentos, prova local e botao de contato imediato.",
        "psychologist": "Com estrutura para psicologia: especialidades, credenciais e botao de contato imediato.",
        "architect": "Com estrutura para arquitetura: portfolio, processo e botao de contato imediato.",
        "financial_advisor": "Com estrutura para consultoria financeira: autoridade, clareza e botao de contato imediato.",
        "insurance_broker": "Com estrutura para corretor de seguros: coberturas, prova local e botao de cotacao imediata.",
        "immigration_consultant": "Com estrutura para consultoria de imigracao: etapas, prova social e botao de contato imediato.",
        "business_consultant": "Com estrutura para consultoria empresarial: diagnostico, casos e botao de contato imediato.",
        "electrician": "Com estrutura para eletricista: prova local, servicos principais e botao de contato imediato.",
        "plumber": "Com estrutura para encanador: prova local, servicos principais e botao de contato imediato.",
        "locksmith": "Com estrutura para chaveiro: prova local, servicos principais e botao de contato imediato.",
        "hvac": "Com estrutura para climatizacao: prova local, servicos principais e botao de contato imediato.",
        "general_service": "Com estrutura para prestador de servico: prova local, servicos principais e botao de contato imediato.",
    }
    return mapping_b.get(category, mapping_b["general_service"]) if ab_variant == "B" else mapping_a.get(category, mapping_a["general_service"])


def _money(amount: int, currency_code: str, locale: str) -> str:
    if _is_pt_br(locale):
        return f"R$ {amount}"
    code = (currency_code or "EUR").strip().upper()
    if code == "EUR":
        return f"EUR {amount}"
    return f"{code} {amount}"


def _en_service_pitch(category: str, has_website: bool) -> str:
    if has_website:
        return "I can redesign your current site into a stronger conversion page with clearer messaging and better CTA flow."
    mapping = {
        "lawyer": "Built for law firms, with trust-first structure and a clear consultation CTA.",
        "accountant": "Built for accountants, with service clarity and a direct CTA for first assessment.",
        "dentist": "Built for dentists, with treatment highlights and booking-focused CTA.",
        "physio": "Built for physiotherapists, with treatment positioning and first-session CTA.",
        "psychologist": "Built for psychologists, with a trust-focused tone and first-contact CTA.",
        "architect": "Built for architects, with portfolio-first layout and briefing CTA.",
        "financial_advisor": "Built for financial advisors, with authority positioning and clear CTA.",
        "insurance_broker": "Built for insurance brokers, with coverage clarity and quote CTA.",
        "immigration_consultant": "Built for immigration consultants, with clear process steps and assessment CTA.",
        "business_consultant": "Built for business consultants, with offer clarity and strategy-call CTA.",
        "electrician": "Built for electricians, with urgency CTA and local proof.",
        "plumber": "Built for plumbers, with emergency CTA and local proof.",
        "locksmith": "Built for locksmiths, with emergency CTA and trust signals.",
        "hvac": "Built for HVAC services, with maintenance CTA and local proof.",
        "general_service": "Built for local services, with simple structure and fast-contact CTA.",
    }
    return mapping.get(category, mapping["general_service"])


def _es_service_pitch(category: str, has_website: bool) -> str:
    if has_website:
        return "Puedo rediseñar su web actual para que convierta mejor, con mensaje más claro y CTA más fuerte."
    mapping = {
        "lawyer": "Pensada para despachos legales, con estructura de confianza y CTA de consulta.",
        "accountant": "Pensada para contables, con servicios claros y CTA para primera evaluación.",
        "dentist": "Pensada para clínicas dentales, con especialidades y CTA de cita.",
        "physio": "Pensada para fisioterapia, con enfoque en tratamientos y CTA de primera evaluación.",
        "psychologist": "Pensada para psicología, con tono de confianza y CTA de primer contacto.",
        "architect": "Pensada para arquitectura, con portafolio visible y CTA de briefing.",
        "financial_advisor": "Pensada para asesoría financiera, con posicionamiento profesional y CTA directo.",
        "insurance_broker": "Pensada para seguros, con claridad de coberturas y CTA de cotización.",
        "immigration_consultant": "Pensada para inmigración, con pasos claros y CTA de evaluación del caso.",
        "business_consultant": "Pensada para consultoría de negocios, con propuesta clara y CTA de reunión.",
        "electrician": "Pensada para electricistas, con CTA de urgencia y prueba local.",
        "plumber": "Pensada para fontaneros, con CTA de urgencia y prueba local.",
        "locksmith": "Pensada para cerrajeros, con CTA de urgencia y señales de confianza.",
        "hvac": "Pensada para climatización, con CTA de mantenimiento y prueba local.",
        "general_service": "Pensada para servicios locales, con estructura simple y CTA de contacto rápido.",
    }
    return mapping.get(category, mapping["general_service"])


def initial_consent_email(
    name: str,
    unsubscribe_url: str,
    variant: int = 1,
    city: str = "",
    has_website: bool = False,
    locale: str = "en",
    service_hint: str = "",
    ab_variant: str = "A",
) -> tuple[str, str, str]:
    _ = variant
    ab = "B" if str(ab_variant or "").strip().upper() == "B" else "A"
    category = _service_category(service_hint)
    if _is_pt_br(locale):
        subject = f"{name}: posso te enviar uma pagina de vendas gratuita?"
    elif _is_spanish(locale):
        subject = f"{name}: puedo enviarte una idea gratuita para tu web?"
    else:
        subject = f"{name}: can I send you a free homepage concept?"
    city_hint = city.strip() or "your area"
    if _is_pt_br(locale):
        city_hint = city.strip() or "sua regiao"
        positioning = _pt_service_pitch(category, ab, has_website)
        open_line = (
            f"Encontrei a ficha de voces no Google em {city_hint} e vi uma oportunidade clara de transformar isso em mais clientes pra voces."
            if ab == "A"
            else f"Vi a ficha de voces no Google em {city_hint} e da para melhorar bastante a conversao disso para novos clientes."
        )
        closing_line = (
            "Se topar ver essa versao gratis, so me dizer um OK que eu ja te mando!"
            if ab == "A"
            else "Se fizer sentido, me responde com um OK que eu te envio a versao gratuita ainda hoje."
        )
        body = (
            f"Oi, equipe {name}.\n\n"
            f"{open_line}\n\n"
            "Eu consigo criar uma pagina de vendas para voces, com visual profissional, carregamento rapido e estrutura pensada para converter em mensagem, ligacao e WhatsApp.\n\n"
            f"{positioning}\n\n"
            "Se quiser, eu monto uma versao gratuita inicialmente pra ver se gostam para o negocio de voces e te envio hoje, sem compromisso.\n\n"
            f"{closing_line}\n\n"
            "Se nao quiser mais mensagens, descadastre aqui: "
            f"{unsubscribe_url}"
        )
    elif _is_spanish(locale):
        city_hint = city.strip() or "tu zona"
        positioning = _es_service_pitch(category, has_website)
        body = (
            f"Hola, equipo de {name}.\n\n"
            f"Encontré su ficha en Google en {city_hint}. {positioning}\n\n"
            "Puedo montar una version concepto gratuita y enviarla hoy, sin compromiso.\n\n"
            "Si no quieren mas mensajes, pueden salir aqui: "
            f"{unsubscribe_url}"
        )
    else:
        positioning = _en_service_pitch(category, has_website)
        body = (
            f"Hi {name} team,\n\n"
            f"I found your Google Business listing in {city_hint}. {positioning}\n\n"
            "I can build a free concept page and send it over today, no commitment.\n\n"
            "If you do not want more messages, unsubscribe here: "
            f"{unsubscribe_url}"
        )
    html = body.replace("\n", "<br>")
    return subject, body, html


def identity_probe_email(
    name: str,
    service_hint: str = "",
    city: str = "",
    locale: str = "en",
) -> tuple[str, str, str]:
    service_text = _identity_service_label(service_hint, locale)
    city_text = _location_hint(city, locale)
    if _is_pt_br(locale):
        subject = f"por acaso esse e-mail e da equipe {name}?"
        body = (
            f"Oi, por acaso esse e-mail e da equipe {name}?\n\n"
            f"Pesquisei no Google por {service_text} em {city_text} e encontrei esse contato aqui."
        )
    elif _is_spanish(locale):
        subject = f"por casualidad este correo es del equipo de {name}?"
        body = (
            f"Hola, por casualidad este correo es del equipo de {name}?\n\n"
            f"Estaba buscando en Google {service_text} en {city_text} y encontré este contacto."
        )
    else:
        subject = f"is this the right email for {name}?"
        body = (
            f"Hi, is this the right email for {name}?\n\n"
            f"I was searching Google for {service_text} in {city_text} and found this contact."
        )
    return subject, body, body.replace("\n", "<br>")


def initial_consent_whatsapp(name: str, has_website: bool = False, locale: str = "en") -> str:
    if _is_pt_br(locale):
        pitch = (
            "Vi o site atual de voces e consigo fazer uma versao muito mais impactante para conversao."
            if has_website
            else "Vi o perfil de voces no Google e consigo montar uma pagina de alta conversao."
        )
        return (
            f"Oi {name}! {pitch} "
            "Quer que eu te envie uma ideia gratuita? Responde SIM. Para parar, responde PARAR."
        )
    if _is_spanish(locale):
        pitch = (
            "Vi su sitio actual y puedo hacer una version mucho mas impactante para conversion."
            if has_website
            else "Vi su ficha en Google y puedo preparar una pagina de alta conversion."
        )
        return (
            f"Hola {name}. {pitch} "
            "Si quiere, le envio una idea gratuita. Responda SI. Para parar, responda STOP."
        )
    pitch = (
        "I saw your current website and I can build a way more impactful version for conversions."
        if has_website
        else "I found your Google listing and I can build a high-converting page for your business."
    )
    return (
        f"Hi {name}! {pitch} "
        "Want me to send a free concept? Reply YES. To stop messages, reply STOP."
    )


def followup_consent_email(
    name: str,
    unsubscribe_url: str,
    step: int,
    has_website: bool = False,
    locale: str = "en",
) -> tuple[str, str, str]:
    if _is_pt_br(locale):
        subject = f"{name}: 3 ajustes rapidos para gerar mais contatos"
    elif _is_spanish(locale):
        subject = f"{name}: 3 ajustes rapidos para generar mas contactos"
    else:
        subject = f"{name}: 3 fast upgrades to get more enquiries"
    first_line = (
        "Since you already have a site, these are focused on lifting conversion without adding complexity:\n\n"
        if has_website
        else "Here are 3 simple upgrades that usually increase local enquiries:\n\n"
    )
    if _is_pt_br(locale):
        first_line = (
            "Como voces ja tem site, esses ajustes focam em aumentar conversao sem complicar:\n\n"
            if has_website
            else "Aqui vao 3 ajustes simples que normalmente aumentam contatos locais:\n\n"
        )
        body = (
            f"Oi, equipe {name}.\n\n"
            f"{first_line}"
            "1) Titulo claro com servico + regiao no topo.\n"
            "2) CTA forte na primeira dobra.\n"
            "3) Prova local (avaliacoes, bairros atendidos, tempo de resposta).\n\n"
            "Se fizer sentido, eu te envio um conceito gratuito com isso aplicado hoje.\n\n"
            "Opt-out: "
            f"{unsubscribe_url}"
        )
    elif _is_spanish(locale):
        first_line = (
            "Como ya tienen web, estos ajustes buscan subir la conversion sin complicar nada:\n\n"
            if has_website
            else "Aqui van 3 ajustes simples que suelen aumentar los contactos locales:\n\n"
        )
        body = (
            f"Hola, equipo de {name}.\n\n"
            f"{first_line}"
            "1) Titulo claro con servicio + zona en la parte superior.\n"
            "2) CTA fuerte en la primera pantalla.\n"
            "3) Prueba local (reseñas, barrios atendidos, tiempo de respuesta).\n\n"
            "Si le sirve, le envio hoy mismo un concepto gratuito con esto aplicado.\n\n"
            "Salir: "
            f"{unsubscribe_url}"
        )
    else:
        body = (
            f"Hi {name} team,\n\n"
            f"{first_line}"
            "1) Clear headline with service + area in the hero.\n"
            "2) Strong call-to-action above the fold.\n"
            "3) Local proof (reviews, neighborhoods served, response time).\n\n"
            "If helpful, I can send a free concept with this applied today.\n\n"
            "Opt-out: "
            f"{unsubscribe_url}"
        )
    if step >= 2:
        if _is_pt_br(locale):
            subject = f"{name}: encerro por aqui?"
            body = (
                f"Oi, equipe {name}.\n\n"
                "Esse e meu ultimo follow-up sobre o conceito gratuito.\n\n"
                "Se quiser, te envio hoje.\n"
                "Se nao for o momento, eu encerro por aqui.\n\n"
                "Opt-out: "
                f"{unsubscribe_url}"
            )
        elif _is_spanish(locale):
            subject = f"{name}: cierro este tema por aqui?"
            body = (
                f"Hola, equipo de {name}.\n\n"
                "Este es mi ultimo seguimiento sobre el concepto gratuito.\n\n"
                "Si lo quieren, se los envio hoy.\n"
                "Si ahora no es prioridad, cierro aqui sin problema.\n\n"
                "Salir: "
                f"{unsubscribe_url}"
            )
        else:
            subject = f"{name}: should I close this thread?"
            body = (
                f"Hi {name} team,\n\n"
                "This is my last follow-up about the free concept.\n\n"
                "If you want it, I can send it today.\n"
                "If timing is not ideal, I will close this here.\n\n"
                "Opt-out: "
                f"{unsubscribe_url}"
            )
    return subject, body, body.replace("\n", "<br>")


def followup_consent_whatsapp(name: str, step: int, has_website: bool = False, locale: str = "en") -> str:
    _ = has_website
    if _is_pt_br(locale):
        if step >= 2:
            return (
                f"{name}, ultimo toque sobre o conceito gratuito. "
                "Se quiser, responde SIM e te envio hoje. Para parar, responde PARAR."
            )
        return (
            f"{name}, ainda tem interesse em receber o conceito gratuito? "
            "Posso preparar hoje. Para parar mensagens, responde PARAR."
        )
    if _is_spanish(locale):
        if step >= 2:
            return (
                f"{name}, ultimo toque sobre el concepto gratuito. "
                "Si le interesa, responda SI y se lo envio hoy. Para parar, responda STOP."
            )
        return (
            f"{name}, sigue interesado en recibir el concepto gratuito? "
            "Lo puedo preparar hoy. Para parar mensajes, responda STOP."
        )
    if step >= 2:
        return (
            f"{name}, quick last follow-up on the free concept. "
            "If you want it, reply YES and I send it today. To stop, reply STOP."
        )
    return (
        f"{name}, still open to receiving the free concept? "
        "I can prep it quickly today. To stop messages, reply STOP."
    )


def offer_email(
    name: str,
    preview_url: str,
    payment_url: str,
    unsubscribe_url: str,
    has_website: bool = False,
    locale: str = "en",
    currency_code: str = "EUR",
    price_full: int = 200,
    price_simple: int = 100,
    payment_url_full: str = "",
    payment_url_simple: str = "",
) -> tuple[str, str, str]:
    subject = f"{name}: concept ready + 2 options"
    if _is_pt_br(locale):
        subject = f"{name}: conceito pronto + 2 opcoes"
    elif _is_spanish(locale):
        subject = f"{name}: concepto listo + 2 opciones"
    payment_block = ""
    if payment_url_full.strip() or payment_url_simple.strip():
        payment_block = (
            "\n\nDirect payment links:\n"
            f"- COMPLETE: {payment_url_full or '-'}\n"
            f"- SIMPLE: {payment_url_simple or '-'}"
        )
    elif payment_url.strip():
        payment_block = (
            "\n\nIf you prefer, I can also send a direct payment link:\n"
            f"{payment_url}"
        )
    if _is_pt_br(locale):
        framing = (
            "Estruturado como uma versao mais forte e focada em conversao do site atual."
            if has_website
            else "Estruturado para dar presenca digital forte e converter melhor."
        )
        body = (
            f"Perfeito, equipe {name}.\n\n"
            f"Seu conceito ficou pronto:\n{preview_url}\n\n"
            f"{framing}\n\n"
            "Proposta:\n\n"
            f"Completo: {_money(price_full, currency_code, locale)}\n"
            f"Simples: {_money(price_simple, currency_code, locale)}\n\n"
            "Os dois incluem publicacao e 1 ano no ar.\n\n"
            "Se quiser seguir, responde \"COMPLETO\" ou \"SIMPLES\" que eu publico em seguida."
            f"{payment_block}\n\n"
            "Opt-out: "
            f"{unsubscribe_url}"
        )
    elif _is_spanish(locale):
        framing = (
            "Lo armé como una version mas fuerte y enfocada en conversion de su web actual."
            if has_website
            else "Lo armé para darle una presencia digital clara y convertir mejor."
        )
        body = (
            f"Perfecto, equipo de {name}.\n\n"
            f"Su concepto ya está listo:\n{preview_url}\n\n"
            f"{framing}\n\n"
            "Propuesta:\n\n"
            f"Completo: {_money(price_full, currency_code, locale)}\n"
            f"Simple: {_money(price_simple, currency_code, locale)}\n\n"
            "Los dos incluyen publicación y 1 año online.\n\n"
            "Si quieren seguir, respondan \"COMPLETO\" o \"SIMPLE\" y lo publico enseguida."
            f"{payment_block}\n\n"
            "Salir: "
            f"{unsubscribe_url}"
        )
    else:
        framing = (
            "Built as a stronger, more conversion-focused upgrade to your current site."
            if has_website
            else "Built to give your business a clear, high-converting online presence."
        )
        body = (
            f"Awesome, {name} team.\n\n"
            f"Your concept is ready:\n{preview_url}\n\n"
            f"{framing}\n\n"
            "Offer:\n\n"
            f"Complete: {_money(price_full, currency_code, locale)}\n"
            f"Simple: {_money(price_simple, currency_code, locale)}\n\n"
            "Both include publishing and 1 year live hosting. "
            "Complete includes deeper conversion structure and stronger content blocks.\n\n"
            "To proceed, reply \"COMPLETE\" or \"SIMPLE\" and I publish right away."
            f"{payment_block}\n\n"
            "Opt-out: "
            f"{unsubscribe_url}"
        )
    html = body.replace("\n", "<br>")
    return subject, body, html


def offer_followup_email(
    name: str,
    unsubscribe_url: str,
    step: int,
    has_website: bool = False,
    locale: str = "en",
) -> tuple[str, str, str]:
    _ = has_website
    if step <= 1:
        if _is_pt_br(locale):
            subject = f"{name}: quer que eu publique hoje?"
            body = (
                f"Oi, equipe {name}.\n\n"
                "Consigo publicar a versao final hoje.\n"
                "Responde com \"COMPLETO\" ou \"SIMPLES\" que eu executo em seguida.\n\n"
                "Opt-out: "
                f"{unsubscribe_url}"
            )
        elif _is_spanish(locale):
            subject = f"{name}: quiere que lo publique hoy?"
            body = (
                f"Hola, equipo de {name}.\n\n"
                "Puedo publicar la version final hoy.\n"
                "Respondan con \"COMPLETO\" o \"SIMPLE\" y lo ejecuto enseguida.\n\n"
                "Salir: "
                f"{unsubscribe_url}"
            )
        else:
            subject = f"{name}: want me to publish today?"
            body = (
                f"Hi {name} team,\n\n"
                "I can publish your final version today.\n"
                "Reply with \"COMPLETE\" or \"SIMPLE\" and I execute immediately.\n\n"
                "Opt-out: "
                f"{unsubscribe_url}"
            )
    else:
        if _is_pt_br(locale):
            subject = f"{name}: finalizamos hoje?"
            body = (
                f"Oi, equipe {name}.\n\n"
                "Fecho esse ciclo hoje para nao te incomodar demais.\n"
                "Se quiser publicar, responde \"COMPLETO\" ou \"SIMPLES\" ate o fim do dia.\n\n"
                "Se nao for o momento, sem problema, eu encerro por aqui.\n\n"
                "Opt-out: "
                f"{unsubscribe_url}"
            )
        elif _is_spanish(locale):
            subject = f"{name}: lo cerramos hoy?"
            body = (
                f"Hola, equipo de {name}.\n\n"
                "Cierro este ciclo hoy para no insistir de más.\n"
                "Si quieren publicar, respondan \"COMPLETO\" o \"SIMPLE\" antes de terminar el día.\n\n"
                "Si ahora no es el momento, no pasa nada, lo cierro aquí.\n\n"
                "Salir: "
                f"{unsubscribe_url}"
            )
        else:
            subject = f"{name}: should we finalize this today?"
            body = (
                f"Hi {name} team,\n\n"
                "I will close this cycle today so I do not over-message you.\n"
                "If you want to publish, reply \"COMPLETE\" or \"SIMPLE\" by end of day.\n\n"
                "If timing is not ideal, no worries, I will close this thread.\n\n"
                "Opt-out: "
                f"{unsubscribe_url}"
            )
    return subject, body, body.replace("\n", "<br>")


def offer_whatsapp(
    name: str,
    preview_url: str,
    payment_url: str,
    has_website: bool = False,
    price_full: int = 100,
    price_simple: int = 50,
    locale: str = "en",
    currency_code: str = "EUR",
) -> str:
    if _is_pt_br(locale):
        framing = (
            "Fiz esse conceito como upgrade de conversao do site atual."
            if has_website
            else "Fiz esse conceito para destacar seu negocio e converter melhor."
        )
        return (
            f"{name}, seu conceito ficou pronto: {preview_url}\n\n"
            f"{framing}\n\n"
            f"Opcoes para publicar hoje:\n- COMPLETO ({_money(price_full, currency_code, locale)})\n- SIMPLES ({_money(price_simple, currency_code, locale)})\n\n"
            f"Quer o link de pagamento agora? {payment_url}\n\n"
            "Para parar mensagens, responda PARAR."
        )
    if _is_spanish(locale):
        framing = (
            "Hice este concepto como una mejora de conversion de su sitio actual."
            if has_website
            else "Hice este concepto para destacar su negocio y convertir mejor."
        )
        return (
            f"{name}, su concepto ya está listo: {preview_url}\n\n"
            f"{framing}\n\n"
            f"Opciones para publicarlo hoy:\n- COMPLETO ({_money(price_full, currency_code, locale)})\n- SIMPLE ({_money(price_simple, currency_code, locale)})\n\n"
            f"Si quiere, le mando el link de pago ahora: {payment_url}\n\n"
            "Para parar mensajes, responda STOP."
        )
    framing = (
        "I built this as a stronger conversion-focused upgrade to your current site."
        if has_website
        else "I built this to make your business stand out and convert better."
    )
    return (
        f"{name}, your concept is ready: {preview_url}\n\n"
        f"{framing}\n\n"
        f"Options to publish today:\n- COMPLETE ({_money(price_full, currency_code, locale)})\n- SIMPLE ({_money(price_simple, currency_code, locale)})\n\n"
        f"Want the payment link now? {payment_url}\n\n"
        "To stop messages, reply STOP."
    )


def is_positive_reply(text: str) -> bool:
    t = (text or "").lower()
    positives = [
        "yes",
        "sounds good",
        "interested",
        "let's do it",
        "go ahead",
        "send it",
        "ok",
        "sure",
        "sim",
        "pode",
        "quero",
    ]
    return any(p in t for p in positives)


def is_opt_out_reply(text: str) -> bool:
    t = (text or "").strip().lower()
    return t in {"parar", "sair", "stop", "unsubscribe", "cancelar", "remove"}


def classify_reply(text: str) -> tuple[str, float]:
    t = (text or "").lower()
    if is_opt_out_reply(t):
        return "opt_out", 0.99
    if is_positive_reply(t):
        return "positive", 0.85
    if "price" in t or "expensive" in t or "preco" in t or "caro" in t:
        return "objection_price", 0.8
    if "later" in t or "not now" in t or "depois" in t or "agora nao" in t:
        return "not_now", 0.8
    if "trust" in t or "guarantee" in t or "confi" in t or "garantia" in t:
        return "objection_trust", 0.75
    return "neutral", 0.5


def classify_identity_reply(text: str, from_email: str = "", from_raw: str = "") -> tuple[str, float]:
    t = (text or "").strip().lower()
    compact = re.sub(r"\s+", " ", t)
    sender = f"{from_email} {from_raw}".strip().lower()
    def contains_any(text_value: str, phrases: list[str]) -> bool:
        return any(phrase in text_value for phrase in phrases)

    def contains_word(text_value: str, word: str) -> bool:
        return re.search(rf"\\b{re.escape(word)}\\b", text_value) is not None

    auto_tokens = [
        "automatic reply",
        "auto reply",
        "autoresponder",
        "out of office",
        "i am away",
        "vacation",
        "thank you for your email",
        "resposta automatica",
        "resposta automática",
        "fora do escritorio",
        "fora do escritório",
        "autorespuesta",
        "respuesta automatica",
        "respuesta automática",
        "fuera de la oficina",
    ]
    shared_tokens = [
        "shared inbox",
        "shared mailbox",
        "team inbox",
        "group inbox",
        "this mailbox is monitored by",
        "this email is managed by",
        "varias personas",
        "varios miembros",
        "caixa compartilhada",
        "esse e-mail e compartilhado",
        "este correo lo revisamos",
        "revisamos este correo entre",
        "several of us",
        "we all monitor",
        "sales team",
        "booking team",
    ]
    yes_tokens = [
        "sim",
        "yes",
        "si",
        "sí",
        "this is",
        "speaking",
        "sou eu",
        "sou da equipe",
        "pode falar",
        "pode mandar",
        "isso mesmo",
        "correct",
        "that is us",
        "this is the right email",
        "soy yo",
        "si, somos nosotros",
        "sí, somos nosotros",
        "este es el correo correcto",
    ]
    no_phrases = [
        "wrong email",
        "wrong person",
        "not me",
        "not us",
        "nao sou",
        "não sou",
        "email errado",
        "contato errado",
        "correo equivocado",
        "correo incorrecto",
        "persona equivocada",
        "este no es",
        "este não é",
        "esse nao e",
        "esse não é",
    ]
    if contains_any(compact, auto_tokens):
        return "auto_reply", 0.98
    if contains_any(compact, shared_tokens):
        return "shared_inbox", 0.82
    if any(prefix in sender for prefix in ["info@", "hello@", "contact@", "sales@", "booking@", "reservas@", "comercial@"]):
        if contains_any(compact, ["correct", "this is", "somos", "podem falar", "pueden hablar"]) or any(
            contains_word(compact, short) for short in ["sim", "si", "sí", "yes"]
        ):
            return "shared_inbox", 0.7
    if contains_any(compact, no_phrases) or any(contains_word(compact, short) for short in ["nao", "não", "no"]):
        return "negative", 0.92
    if contains_any(compact, [token for token in yes_tokens if len(token) > 3]) or any(
        contains_word(compact, short) for short in ["sim", "si", "sí", "yes"]
    ):
        return "positive", 0.9
    if compact in {"ok", "okay", "certo", "quem fala?", "who is this?", "quien habla?", "quién habla?"}:
        return "ambiguous_human", 0.45
    return "ambiguous_human", 0.3


def detect_auto_reply_inbound(text: str, subject: str = "", payload: dict | None = None) -> tuple[bool, str]:
    content = f"{subject or ''}\n{text or ''}".strip().lower()
    auto_tokens = [
        "automatic reply",
        "auto reply",
        "autoresponder",
        "out of office",
        "i am away",
        "vacation",
        "thank you for your email",
        "resposta automatica",
        "resposta automática",
        "fora do escritorio",
        "fora do escritório",
        "autorespuesta",
        "respuesta automatica",
        "respuesta automática",
        "fuera de la oficina",
        "i'll be back",
        "i will be back",
        "não estou no escritório",
        "nao estou no escritorio",
    ]
    for token in auto_tokens:
        if token in content:
            return True, f"token:{token}"

    data = payload or {}
    if isinstance(data, dict):
        headers = data.get("headers") or data.get("email_headers") or {}
        header_blob = ""
        if isinstance(headers, dict):
            header_blob = "\n".join(f"{k}:{v}" for k, v in headers.items()).lower()
        elif isinstance(headers, list):
            header_blob = "\n".join(str(x) for x in headers).lower()
        raw_blob = f"{header_blob}\n{str(data.get('raw_headers') or '').lower()}"
        header_tokens = [
            "auto-submitted:auto-replied",
            "x-autoreply",
            "x-autorespond",
            "x-auto-response-suppress",
            "precedence: bulk",
            "precedence: list",
            "precedence: junk",
        ]
        for token in header_tokens:
            if token in raw_blob:
                return True, f"header:{token}"

    return False, ""


def detect_plan_choice(text: str) -> str:
    t = (text or "").strip().lower()
    if any(k in t for k in ["simple", "simples", "plano simples", "50", "100"]):
        return "SIMPLES"
    if any(k in t for k in ["complete", "completo", "plano completo"]):
        return "COMPLETO"
    return "COMPLETO"


def classify_codex_intent(text: str) -> str:
    t = (text or "").lower()
    if is_opt_out_reply(t):
        return "opt_out"
    if any(k in t for k in ["complete", "simple", "completo", "simples", "closed", "deal", "pode publicar", "vamos fechar"]):
        return "positive_offer_accept"
    if "price" in t or "expensive" in t or "preco" in t or "caro" in t:
        return "objection_price"
    if "trust" in t or "guarantee" in t or "confi" in t or "garantia" in t:
        return "objection_trust"
    if "later" in t or "not now" in t or "depois" in t or "agora nao" in t:
        return "not_now"
    return "other"


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

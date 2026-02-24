from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass
class DemoSiteResult:
    preview_url: str
    file_path: Path


class DemoSiteBuilder:
    def __init__(self, base_url: str, publish_dir: Path) -> None:
        self.base_url = base_url.rstrip("/")
        self.publish_dir = publish_dir
        self.publish_dir.mkdir(parents=True, exist_ok=True)

    def build_for_lead(self, lead_slug: str, business_name: str, category: str, city_hint: str) -> DemoSiteResult:
        html = self._generate_html(business_name=business_name, category=category, city_hint=city_hint)
        lead_dir = self.publish_dir / lead_slug
        lead_dir.mkdir(parents=True, exist_ok=True)
        out_file = lead_dir / "index.html"
        out_file.write_text(html, encoding="utf-8")
        preview_url = f"{self.base_url}/preview/{lead_slug}/"
        return DemoSiteResult(preview_url=preview_url, file_path=out_file)

    def _generate_html(self, business_name: str, category: str, city_hint: str) -> str:
        ai = self._generate_copy_openai(business_name, category, city_hint)
        if ai:
            return ai
        title = f"{business_name} - Atendimento rapido e profissional"
        return f"""<!doctype html>
<html lang='pt-BR'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <title>{_esc(title)}</title>
  <style>
    :root {{ --bg:#f6f8f5; --ink:#14211b; --acc:#0a7f48; --card:#ffffff; }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family: 'Segoe UI', sans-serif; color:var(--ink); background: radial-gradient(circle at top right,#d8f2e5,transparent 40%), var(--bg); }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 28px 18px 70px; }}
    .hero {{ background:var(--card); border-radius:18px; padding:30px; box-shadow:0 14px 40px rgba(0,0,0,.08); }}
    h1 {{ font-size: clamp(30px, 5vw, 50px); line-height:1.05; margin:0 0 14px; }}
    p {{ font-size:18px; line-height:1.55; }}
    .cta {{ display:inline-block; margin-top:18px; padding:14px 20px; background:var(--acc); color:#fff; text-decoration:none; border-radius:999px; font-weight:700; }}
    .grid {{ display:grid; gap:14px; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); margin-top:26px; }}
    .item {{ background:var(--card); border-radius:14px; padding:18px; box-shadow:0 8px 30px rgba(0,0,0,.07); }}
  </style>
</head>
<body>
  <main class='wrap'>
    <section class='hero'>
      <h1>{_esc(business_name)} pode receber mais pedidos sem depender so do Google Maps</h1>
      <p>Pagina demo criada para <strong>{_esc(category or 'prestadores de servico')}</strong> em { _esc(city_hint or 'sua regiao') }. 
      Com foco em resposta rapida no WhatsApp e formulario simples para novos clientes.</p>
      <a class='cta' href='https://wa.me/' target='_blank' rel='noreferrer'>Quero atendimento agora</a>
    </section>
    <section class='grid'>
      <article class='item'><h3>Resposta em minutos</h3><p>CTA direto para WhatsApp para reduzir abandono.</p></article>
      <article class='item'><h3>Prova de confianca</h3><p>Bloco de depoimentos e diferenciais de atendimento.</p></article>
      <article class='item'><h3>Captura de leads</h3><p>Formulario enxuto para pedidos e orcamentos.</p></article>
    </section>
  </main>
</body>
</html>"""

    def _generate_copy_openai(self, business_name: str, category: str, city_hint: str) -> str:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            return ""

        prompt = (
            "Gere um HTML unico, completo e responsivo para uma landing page em portugues-BR. "
            "Nao use markdown. Sem scripts externos. "
            f"Negocio: {business_name}. Categoria: {category or 'prestador de servico'}. Local: {city_hint}. "
            "A pagina deve ter hero, beneficios, prova social ficticia e CTA para WhatsApp."
        )
        payload = {
            "model": os.getenv("OPENAI_MODEL", "gpt-5-mini"),
            "input": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        }
        req = Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(req, timeout=40) as res:
                body = json.loads(res.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError):
            return ""

        text = _extract_output_text(body)
        if "<html" not in text.lower():
            return ""
        return text


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", (value or "").strip().lower())
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "lead"


def _extract_output_text(payload: dict) -> str:
    out = payload.get("output", [])
    parts: list[str] = []
    for item in out:
        content = item.get("content", []) if isinstance(item, dict) else []
        for c in content:
            if isinstance(c, dict) and c.get("type") in {"output_text", "text"}:
                txt = c.get("text", "")
                if isinstance(txt, str):
                    parts.append(txt)
    return "\n".join(parts).strip()


def _esc(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

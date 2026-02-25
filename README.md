# LeadGenerator (Google Maps -> Consentimento -> Demo -> Oferta)

Servico de leadgen com foco em negocios no Google Maps sem website, com protecao anti-ban e outreach por email (Resend), com cadencia de 7 dias, precificacao adaptativa e revisao de respostas via fila Codex.

## O que foi implementado

- Scraper Google Maps com anti-ban pratico:
  - delay aleatorio por acao
  - pausa longa a cada bloco de resultados
  - deteccao de risco (captcha/timeout/429)
  - pausa automatica `SCRAPE_PAUSED` por streak de erro
- Pipeline operacional com estado em SQLite:
  - `logs/pipeline_state.db` (leads, touches, replies, opt-outs)
  - `logs/ops_state.db` (status de canais, metricas diarias, safe mode)
- Outreach consent-first:
  - primeiro contato sem links
  - email via Resend
  - fallback WhatsApp via WPPConnect quando nao houver email
- Opt-out obrigatorio:
  - unsubscribe por link no email
  - comando `PARAR/STOP` no WhatsApp
  - supressao por hash de contato
- Geracao de demo custom por lead:
  - publica em diretorio de preview no servidor
  - usa OpenAI quando `OPENAI_API_KEY` existe
  - fallback local sem IA quando necessario
- Envio de oferta apos aceite:
  - preview + pagamento
- Escada de preco adaptativa:
  - inicio em R$200 (completo) / R$100 (simples)
  - sobe +100 por venda
  - desce 1 nivel quando janela de 10 ofertas nao sustenta conversao
- Fila obrigatoria de revisao inbound:
  - 100% dos inbounds entram em `reply_review_queue`
  - sem resposta comercial sem decisao Codex
- Operacao de dominio por venda:
  - cria `domain_jobs` com checklist
  - alerta de expiracao em 30/15/7 dias
- Kill-switch automatico:
  - `EMAIL_PAUSED`, `WHATSAPP_PAUSED`, `SCRAPE_PAUSED`
  - `GLOBAL_SAFE_MODE` quando >= 2 canais pausados
- Logs JSONL em UTC com redaction automatica de segredos
- Fingerprint de incidente e escalonamento L0-L3
- Script de deploy seguro DigitalOcean com rollback

## Instalacao local

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env
```

## Comando legado (so scraping + export)

```bash
python scripts/run_leadgen.py \
  --audience "eletricistas" \
  --location "Sao Paulo SP" \
  --max-results 60 \
  --format both \
  --enrich-website
```

## Pipeline completo

### One-shot

```bash
python scripts/run_pipeline.py run-all \
  --audience "encanadores" \
  --location "Sao Paulo SP" \
  --max-results 60 \
  --payment-url "https://pay.example/checkout/abc"
```

### Etapas separadas

```bash
python scripts/run_pipeline.py ingest --audience "eletricistas" --location "Sao Paulo SP"
python scripts/run_pipeline.py outreach --run-id manual
python scripts/run_pipeline.py followups --run-id manual
python scripts/run_pipeline.py reply --lead-id 42 --channel WHATSAPP --text "sim pode enviar"
python scripts/run_pipeline.py offers --run-id manual --payment-url "https://pay.example/checkout/abc"
python scripts/run_pipeline.py email-feedback --sent 120 --bounces 8 --complaints 1
python scripts/run_pipeline.py sales-mark --lead-id 42 --accepted-plan COMPLETO --run-id manual
python scripts/run_pipeline.py close-stale --run-id manual
```

## Politica anti-ban aplicada

- Scraping:
  - 1 sessao ativa por run
  - delay aleatorio (1.8s-4.2s)
  - pausa longa por bloco de 20
  - pausa do canal por 12h em caso de erro repetido/captcha
- Email:
  - warm-up diario (30 -> 60 -> +20 por degrau)
  - pausa automatica com bounce > 5% ou complaint > 0.3%
- WhatsApp:
  - somente fallback quando email ausente
  - pausa automatica quando falha > 10%
- Kill-switch:
  - quando 2+ canais pausados, entra `GLOBAL_SAFE_MODE`
  - safe mode bloqueia novos envios, mantendo ingestao e processamento

## Logs e incidentes

- Eventos: `logs/events.jsonl`
- Incidentes: `logs/incident_state.db` + `logs/incidents/incident-*.md`
- Estado operacional: `logs/ops_state.db`
- Estado CRM/pipeline: `logs/pipeline_state.db`

Eventos adicionais importantes:
- `channel_paused`, `channel_resumed`
- `safe_mode_enabled`, `safe_mode_disabled`
- `captcha_detected`, `deliverability_alert`
- `contact_delivered`, `contact_failed`, `reply_received`, `opt_out_registered`

## Deploy no DigitalOcean

```bash
export DO_SSH_TARGET="root@SEU_IP"
export REMOTE_DIR="/opt/leadgenerator"
export SERVICE_NAME="leadgenerator"
export PRE_HEALTHCHECK_URL="https://seu-backend/health"
export POST_HEALTHCHECK_URL="https://seu-backend/health"
export EXPO_CHECK_URL="https://seu-expo/status"

bash scripts/deploy_do_safe.sh
```

O script executa: precheck -> backup -> deploy -> postcheck -> expo check -> rollback automatico em falha.

## Webhook inbound (Resend)

Endpoint esperado para respostas de email:

- `POST /webhooks/resend-inbound`

Esse endpoint classifica a resposta e atualiza o lead:

- `positive` -> `CONSENTED`
- `opt_out` -> `UNSUBSCRIBED`
- demais -> `WAITING_REPLY`

## Stripe (checkout + webhook)

- Webhook de pagamento:
  - `POST /webhooks/stripe`
  - evento esperado: `checkout.session.completed`
- Endpoint utilitario para gerar checkout manual:
  - `POST /api/payments/checkout` com `{ "lead_id": 42, "plan": "COMPLETO" }`

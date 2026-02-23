# LeadGenerator (Google Maps -> Planilha)

Servico para gerar leads a partir de buscas no Google Maps e exportar CSV/XLSX.

## O que foi implementado

- Scraper com Playwright para Google Maps (publico + local)
- Enriquecimento opcional por website usando Scrapling (quando instalado)
- Exportacao para CSV e XLSX
- Logs estruturados JSONL em UTC (`logs/events.jsonl`)
- Redaction automatica de segredos antes de gravar log
- Fingerprint de erro (`error_type + message + stack + contexto`)
- Escalonamento de incidente (L0, L1, L2, L3)
- Relatorio automatico `incident-<fingerprint>-<timestamp>.md` para incidentes >= L2
- Script de deploy seguro para DigitalOcean com:
  - precheck
  - backup remoto
  - deploy
  - postcheck
  - validacao Expo
  - rollback automatico em falha

## Instalacao local

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env
```

### Opcional: enriquecimento com Scrapling

Para extrair contatos adicionais (email/telefone) dos websites dos leads:

```bash
pip install -r requirements-optional.txt
```

## Uso

```bash
source .venv/bin/activate
python scripts/run_leadgen.py \
  --audience "dentistas" \
  --location "Sao Paulo SP" \
  --max-results 50 \
  --format both \
  --enrich-website
```

Saida em `output/`.

## Logs e incidentes

- Eventos: `logs/events.jsonl`
- Estado de incidente: `logs/incident_state.db`
- Relatorios: `logs/incidents/incident-*.md`
- Enriquecimento: eventos `lead_enriched` e `lead_enrichment_failed` com provider usado

## Deploy no DigitalOcean (Droplet)

1. Configure `.env` remoto e unit file do systemd.
2. Exporte variaveis locais:

```bash
export DO_SSH_TARGET="root@SEU_IP"
export REMOTE_DIR="/opt/leadgenerator"
export SERVICE_NAME="leadgenerator"
export PRE_HEALTHCHECK_URL="https://seu-backend/health"
export POST_HEALTHCHECK_URL="https://seu-backend/health"
export EXPO_CHECK_URL="https://seu-expo/status"
```

3. Execute deploy:

```bash
bash scripts/deploy_do_safe.sh
```

## Observacoes importantes

- Google Maps pode alterar seletores e limitar automacao. Se quebrar, ajuste `src/leadgen/scraper.py`.
- Para volume alto e estabilidade, considere fallback com Places API oficial.
- Se Scrapling nao estiver instalado/indisponivel, o sistema faz fallback automatico para `urllib` no enriquecimento.

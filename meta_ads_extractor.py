"""
Meta Ads API — Script de Extração para TCC
==========================================
Autor: [Acacio Guirra]
Curso: Análise e Desenvolvimento de Sistemas
Descrição: Extrai dados de campanhas, conjuntos e anúncios da Meta Ads API
           e carrega no BigQuery (ou exporta para CSV como fallback).

Dependências:
    pip install facebook-business google-cloud-bigquery pandas python-dotenv

Uso:
    1. Configure o arquivo .env com suas credenciais
    2. Execute: python meta_ads_extractor.py
"""

import os
import sys
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# ─── Facebook Business SDK ────────────────────────────────────────────────────
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError

# ─── Google BigQuery (opcional — comente se usar só CSV) ──────────────────────
try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    print("[AVISO] google-cloud-bigquery não instalado. Usando fallback CSV.")


# ==============================================================================
# CONFIGURAÇÃO DE LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("extractor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ==============================================================================
# CARREGAMENTO DE CREDENCIAIS (.env)
# ==============================================================================

load_dotenv()

APP_ID           = os.getenv("META_APP_ID")
APP_SECRET       = os.getenv("META_APP_SECRET")
ACCESS_TOKEN     = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID    = os.getenv("META_AD_ACCOUNT_ID")   # Formato: act_XXXXXXXXX

# BigQuery (só necessário se usar BigQuery como destino)
GCP_PROJECT_ID   = os.getenv("GCP_PROJECT_ID")
BQ_DATASET       = os.getenv("BQ_DATASET", "meta_ads")

# Período de extração (padrão: últimos 90 dias)
DAYS_LOOKBACK    = int(os.getenv("DAYS_LOOKBACK", "90"))


def _validate_env() -> None:
    """Valida se todas as variáveis obrigatórias estão presentes."""
    required = {
        "META_APP_ID": APP_ID,
        "META_APP_SECRET": APP_SECRET,
        "META_ACCESS_TOKEN": ACCESS_TOKEN,
        "META_AD_ACCOUNT_ID": AD_ACCOUNT_ID,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error("Variáveis de ambiente ausentes: %s", ", ".join(missing))
        log.error("Crie um arquivo .env na raiz do projeto com essas variáveis.")
        sys.exit(1)


# ==============================================================================
# INICIALIZAÇÃO DA API
# ==============================================================================

def init_api() -> AdAccount:
    """Inicializa a conexão com a Meta Ads API e retorna o objeto AdAccount."""
    FacebookAdsApi.init(
        app_id=APP_ID,
        app_secret=APP_SECRET,
        access_token=ACCESS_TOKEN,
    )
    account = AdAccount(AD_ACCOUNT_ID)
    log.info("API inicializada — conta: %s", AD_ACCOUNT_ID)
    return account


# ==============================================================================
# DEFINIÇÃO DOS CAMPOS E MÉTRICAS
# ==============================================================================

# Campos de insights agregados por dia (nível campanha/conjunto/anúncio)
INSIGHT_FIELDS = [
    "date_start",
    "date_stop",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "spend",               # Investimento (R$)
    "impressions",         # Impressões
    "reach",               # Alcance (pessoas únicas)
    "clicks",              # Cliques totais
    "unique_clicks",       # Cliques únicos
    "ctr",                 # CTR (%)
    "cpc",                 # Custo por clique
    "cpm",                 # CPM
    "cpp",                 # Custo por mil pessoas alcançadas
    "frequency",           # Frequência
    "actions",             # Conversões (compras, checkouts, etc.)
    "action_values",       # Valor das conversões (receita)
    "cost_per_action_type", # CPA por tipo de ação
    "video_p25_watched_actions",  # Video View 25%
    "video_p75_watched_actions",  # Video View 75%
    "website_ctr",         # CTR para o site
    "outbound_clicks",     # Cliques no link (saída)
]

# Parâmetros de segmentação por dia
INSIGHT_PARAMS = {
    "level": "ad",                      # Granularidade máxima
    "time_increment": 1,               # 1 = por dia
    "limit": 500,                      # Registros por página
}


# ==============================================================================
# FUNÇÃO DE EXTRAÇÃO COM PAGINAÇÃO E RETRY
# ==============================================================================

def fetch_insights(
    account: AdAccount,
    date_start: str,
    date_stop: str,
    max_retries: int = 3,
) -> list[dict]:
    """
    Extrai insights da API com suporte a paginação automática e retry.

    Args:
        account:     Objeto AdAccount autenticado.
        date_start:  Data inicial no formato YYYY-MM-DD.
        date_stop:   Data final no formato YYYY-MM-DD.
        max_retries: Número máximo de tentativas em caso de erro de rate limit.

    Returns:
        Lista de dicionários com os dados brutos da API.
    """
    params = {
        **INSIGHT_PARAMS,
        "time_range": {"since": date_start, "until": date_stop},
        "filtering": [],
    }

    log.info("Iniciando extração: %s → %s", date_start, date_stop)
    all_rows = []
    attempt = 0

    while attempt < max_retries:
        try:
            cursor = account.get_insights(fields=INSIGHT_FIELDS, params=params)

            # Paginação automática
            page_num = 0
            while True:
                page_num += 1
                batch = [row.export_all_data() for row in cursor]
                all_rows.extend(batch)
                log.info("  Página %d: %d registros (total: %d)", page_num, len(batch), len(all_rows))

                if not cursor.load_next_page():
                    break

            log.info("Extração concluída: %d registros totais", len(all_rows))
            return all_rows

        except FacebookRequestError as e:
            attempt += 1
            # Rate limit (código 17 ou 32)
            if e.api_error_code() in (17, 32, 613):
                wait_time = 60 * attempt
                log.warning(
                    "Rate limit atingido (código %d). Aguardando %ds antes de tentar novamente (%d/%d)...",
                    e.api_error_code(), wait_time, attempt, max_retries
                )
                time.sleep(wait_time)
            else:
                log.error("Erro da API (código %d): %s", e.api_error_code(), e.api_error_message())
                raise

    log.error("Falha após %d tentativas. Abortando.", max_retries)
    raise RuntimeError(f"Extração falhou após {max_retries} tentativas.")


# ==============================================================================
# TRANSFORMAÇÃO E LIMPEZA DOS DADOS
# ==============================================================================

def _extract_action_value(actions: list, action_type: str) -> float:
    """Extrai o valor de uma ação específica da lista de actions da API."""
    if not actions:
        return 0.0
    for action in actions:
        if action.get("action_type") == action_type:
            return float(action.get("value", 0))
    return 0.0


def transform(raw_rows: list[dict]) -> pd.DataFrame:
    """
    Transforma os dados brutos da API em um DataFrame limpo e tipado,
    com métricas derivadas calculadas (ROAS, CPA, taxas de conversão).

    Args:
        raw_rows: Lista de dicts retornada por fetch_insights().

    Returns:
        DataFrame pandas pronto para carga no BigQuery ou exportação CSV.
    """
    if not raw_rows:
        log.warning("Nenhum dado para transformar.")
        return pd.DataFrame()

    rows = []
    for r in raw_rows:
        actions       = r.get("actions", [])
        action_values = r.get("action_values", [])

        # Conversões padrão de e-commerce (ajuste os action_types conforme seu pixel)
        purchases          = _extract_action_value(actions, "purchase")
        purchase_revenue   = _extract_action_value(action_values, "purchase")
        add_to_cart        = _extract_action_value(actions, "add_to_cart")
        initiate_checkout  = _extract_action_value(actions, "initiate_checkout")
        view_content       = _extract_action_value(actions, "view_content")
        link_clicks        = _extract_action_value(actions, "link_click")
        landing_page_views = _extract_action_value(actions, "landing_page_view")
        messages_started   = _extract_action_value(actions, "onsite_conversion.messaging_conversation_started_7d")

        # Video views
        video_25pct = _extract_action_value(r.get("video_p25_watched_actions", []), "video_view")
        video_75pct = _extract_action_value(r.get("video_p75_watched_actions", []), "video_view")

        spend       = float(r.get("spend", 0) or 0)
        impressions = int(r.get("impressions", 0) or 0)
        clicks      = int(r.get("clicks", 0) or 0)
        reach       = int(r.get("reach", 0) or 0)

        # ── Métricas derivadas ──────────────────────────────────────────────
        roas                 = purchase_revenue / spend if spend > 0 else 0.0
        cpa                  = spend / purchases if purchases > 0 else 0.0
        ctr                  = float(r.get("ctr", 0) or 0)
        cpm                  = float(r.get("cpm", 0) or 0)
        cpc                  = float(r.get("cpc", 0) or 0)
        frequency            = float(r.get("frequency", 0) or 0)
        cost_per_checkout    = spend / initiate_checkout if initiate_checkout > 0 else 0.0
        cost_per_message     = spend / messages_started if messages_started > 0 else 0.0

        # Taxas do funil
        connect_rate         = (landing_page_views / clicks * 100) if clicks > 0 else 0.0
        checkout_rate        = (initiate_checkout / landing_page_views * 100) if landing_page_views > 0 else 0.0
        purchase_rate        = (purchases / initiate_checkout * 100) if initiate_checkout > 0 else 0.0
        video_view_25_rate   = (video_25pct / impressions * 100) if impressions > 0 else 0.0
        video_view_75_rate   = (video_75pct / impressions * 100) if impressions > 0 else 0.0

        rows.append({
            # ── Dimensões ──
            "date":              r.get("date_start"),
            "campaign_id":       r.get("campaign_id"),
            "campaign_name":     r.get("campaign_name"),
            "adset_id":          r.get("adset_id"),
            "adset_name":        r.get("adset_name"),
            "ad_id":             r.get("ad_id"),
            "ad_name":           r.get("ad_name"),

            # ── Volume ──
            "impressions":       impressions,
            "reach":             reach,
            "clicks":            clicks,
            "link_clicks":       int(link_clicks),
            "landing_page_views": int(landing_page_views),

            # ── Conversões ──
            "view_content":      int(view_content),
            "add_to_cart":       int(add_to_cart),
            "initiate_checkout": int(initiate_checkout),
            "purchases":         int(purchases),
            "messages_started":  int(messages_started),
            "video_25pct":       int(video_25pct),
            "video_75pct":       int(video_75pct),

            # ── Financeiro ──
            "spend":             round(spend, 2),
            "purchase_revenue":  round(purchase_revenue, 2),

            # ── KPIs derivados ──
            "roas":              round(roas, 4),
            "cpa":               round(cpa, 2),
            "ctr":               round(ctr, 4),
            "cpm":               round(cpm, 2),
            "cpc":               round(cpc, 2),
            "frequency":         round(frequency, 2),
            "cost_per_checkout": round(cost_per_checkout, 2),
            "cost_per_message":  round(cost_per_message, 2),

            # ── Taxas do funil (%) ──
            "connect_rate":      round(connect_rate, 2),
            "checkout_rate":     round(checkout_rate, 2),
            "purchase_rate":     round(purchase_rate, 2),
            "video_view_25_rate": round(video_view_25_rate, 2),
            "video_view_75_rate": round(video_view_75_rate, 2),

            # ── Controle de carga ──
            "extracted_at":      datetime.utcnow().isoformat(),
        })

    df = pd.DataFrame(rows)

    # Tipagem correta
    df["date"] = pd.to_datetime(df["date"])
    int_cols = [
        "impressions", "reach", "clicks", "link_clicks", "landing_page_views",
        "view_content", "add_to_cart", "initiate_checkout", "purchases",
        "messages_started", "video_25pct", "video_75pct",
    ]
    df[int_cols] = df[int_cols].fillna(0).astype(int)

    # Remove duplicatas (segurança)
    df = df.drop_duplicates(subset=["date", "ad_id"])

    log.info("Transformação concluída: %d linhas, %d colunas", len(df), len(df.columns))
    return df


# ==============================================================================
# CARGA — BIGQUERY
# ==============================================================================

BQ_SCHEMA = [
    bigquery.SchemaField("date",              "DATE")         if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("campaign_id",       "STRING")       if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("campaign_name",     "STRING")       if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("adset_id",          "STRING")       if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("adset_name",        "STRING")       if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("ad_id",             "STRING")       if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("ad_name",           "STRING")       if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("impressions",       "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("reach",             "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("clicks",            "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("link_clicks",       "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("landing_page_views","INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("view_content",      "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("add_to_cart",       "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("initiate_checkout", "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("purchases",         "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("messages_started",  "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("video_25pct",       "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("video_75pct",       "INTEGER")      if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("spend",             "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("purchase_revenue",  "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("roas",              "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("cpa",               "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("ctr",               "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("cpm",               "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("cpc",               "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("frequency",         "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("cost_per_checkout", "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("cost_per_message",  "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("connect_rate",      "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("checkout_rate",     "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("purchase_rate",     "FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("video_view_25_rate","FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("video_view_75_rate","FLOAT")        if BIGQUERY_AVAILABLE else None,
    bigquery.SchemaField("extracted_at",      "TIMESTAMP")    if BIGQUERY_AVAILABLE else None,
] if BIGQUERY_AVAILABLE else []

BQ_SCHEMA = [f for f in BQ_SCHEMA if f is not None]


def load_to_bigquery(df: pd.DataFrame, table_id: str = "ad_insights") -> None:
    """
    Carrega o DataFrame no BigQuery usando WRITE_TRUNCATE por partição de data.
    Cria o dataset e a tabela automaticamente se não existirem.

    Args:
        df:       DataFrame transformado.
        table_id: Nome da tabela no BigQuery.
    """
    if not BIGQUERY_AVAILABLE:
        log.error("BigQuery não disponível. Use export_to_csv() como alternativa.")
        return

    if df.empty:
        log.warning("DataFrame vazio. Nada para carregar.")
        return

    client = bigquery.Client(project=GCP_PROJECT_ID)
    full_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_id}"

    # Garante que o dataset existe
    try:
        client.get_dataset(BQ_DATASET)
    except NotFound:
        dataset = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET}")
        dataset.location = "US"
        client.create_dataset(dataset)
        log.info("Dataset criado: %s", BQ_DATASET)

    # Configuração de carga — WRITE_APPEND para acumular histórico
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=False,
    )

    # Remove linhas que já existem para a mesma data (deduplicação)
    dates_in_df = df["date"].dt.strftime("%Y-%m-%d").unique().tolist()
    dates_str   = ", ".join(f"'{d}'" for d in dates_in_df)

    try:
        client.query(
            f"DELETE FROM `{full_table}` WHERE DATE(date) IN ({dates_str})"
        ).result()
        log.info("Deduplicação: removidas linhas existentes para %d datas.", len(dates_in_df))
    except Exception:
        log.info("Tabela ainda não existe ou sem dados anteriores — prosseguindo.")

    # Converte date para string antes de carregar (compatibilidade Parquet → BQ DATE)
    df_bq = df.copy()
    df_bq["date"] = df_bq["date"].dt.strftime("%Y-%m-%d")

    job = client.load_table_from_dataframe(df_bq, full_table, job_config=job_config)
    job.result()

    log.info("Carga concluída: %d linhas → %s", len(df), full_table)


# ==============================================================================
# CARGA — CSV (FALLBACK)
# ==============================================================================

def export_to_csv(df: pd.DataFrame, output_dir: str = "output") -> str:
    """
    Exporta o DataFrame para CSV como alternativa ao BigQuery.
    Útil para testes locais ou quando BigQuery não está configurado.

    Args:
        df:         DataFrame transformado.
        output_dir: Diretório de saída (criado automaticamente).

    Returns:
        Caminho do arquivo gerado.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"meta_ads_insights_{timestamp}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("CSV exportado: %s (%d linhas)", path, len(df))
    return path


# ==============================================================================
# PONTO DE ENTRADA PRINCIPAL
# ==============================================================================

def run(
    date_start: Optional[str] = None,
    date_stop: Optional[str] = None,
    destination: str = "bigquery",   # "bigquery" ou "csv"
) -> pd.DataFrame:
    """
    Executa o pipeline completo: extração → transformação → carga.

    Args:
        date_start:   Data inicial (YYYY-MM-DD). Padrão: hoje - DAYS_LOOKBACK.
        date_stop:    Data final (YYYY-MM-DD). Padrão: ontem.
        destination:  Destino dos dados: "bigquery" ou "csv".

    Returns:
        DataFrame com os dados extraídos e transformados.
    """
    _validate_env()

    # Período padrão
    today     = datetime.today()
    yesterday = today - timedelta(days=1)

    date_start = date_start or (today - timedelta(days=DAYS_LOOKBACK)).strftime("%Y-%m-%d")
    date_stop  = date_stop  or yesterday.strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("INÍCIO DO PIPELINE")
    log.info("Período: %s → %s", date_start, date_stop)
    log.info("Destino: %s", destination.upper())
    log.info("=" * 60)

    # 1. Extração
    account  = init_api()
    raw_data = fetch_insights(account, date_start, date_stop)

    # 2. Transformação
    df = transform(raw_data)

    if df.empty:
        log.warning("Nenhum dado disponível para o período informado.")
        return df

    # 3. Carga
    if destination == "bigquery":
        if BIGQUERY_AVAILABLE and GCP_PROJECT_ID:
            load_to_bigquery(df)
        else:
            log.warning("BigQuery não configurado. Exportando para CSV.")
            export_to_csv(df)
    else:
        export_to_csv(df)

    log.info("PIPELINE FINALIZADO COM SUCESSO")
    return df


# ==============================================================================
# EXECUÇÃO DIRETA
# ==============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extrator Meta Ads API — TCC")
    parser.add_argument("--start",  type=str, help="Data inicial YYYY-MM-DD")
    parser.add_argument("--end",    type=str, help="Data final YYYY-MM-DD")
    parser.add_argument("--dest",   type=str, default="csv",
                        choices=["bigquery", "csv"],
                        help="Destino dos dados (default: csv)")
    args = parser.parse_args()

    df_result = run(
        date_start=args.start,
        date_stop=args.end,
        destination=args.dest,
    )

    if not df_result.empty:
        print("\n── Prévia dos dados extraídos ──")
        print(df_result[["date", "campaign_name", "spend", "purchases", "roas", "ctr"]].head(10).to_string(index=False))
        print(f"\nTotal de linhas: {len(df_result)}")

#!/usr/bin/env python3
"""
Bot de captura do IPCA (SIDRA/IBGE) e gravação em Parquet.
"""
import argparse, json, logging
from pathlib import Path
from typing import List, Dict
import pandas as pd, requests

SIDRA_URLS = [
    "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/all/p/last%20120?formato=json",
    "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/all/p/all?formato=json",
    "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1",
]

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUT_DIR  = BASE_DIR / "output"
FIXTURE_FILE = DATA_DIR / "sample_ipca.json"

def setup_logging(): logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def _make_session():
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"], raise_on_status=False)
    s = requests.Session()
    s.headers.update({"User-Agent": "ipca-bot/1.0 (+colab)"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

def fetch_json(url: str):
    s = _make_session()
    logging.info("Baixando dados: %s", url)
    r = s.get(url, timeout=30); r.raise_for_status()
    try: return r.json()
    except Exception:
        import json as _j; return _j.loads(r.text)

def fetch_json_with_fallback():
    import logging

    last_err=None
    for u in SIDRA_URLS:
        try: return fetch_json(u)
        except Exception as e:
            last_err=e; logging.warning("Falha em %s: %s", u, e)
    raise last_err

def load_fixture(path: Path) -> List[Dict]:
    logging.info("Carregando fixture local: %s", path)
    with open(path,"r",encoding="utf-8") as f: return json.load(f)


def normalize_ipca(payload) -> pd.DataFrame:
    import logging

    """
    Normaliza resposta da API v2 (lista em que o item 0 é o cabeçalho).
    Faz parsing do período usando D3C (AAAAMM) ou D3N ('jan/2025', etc).
    """
    import pandas as pd, re

    mes_pt = {"jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,
              "jul":7,"ago":8,"set":9,"out":10,"nov":11,"dez":12}

    def parse_period(item):
        # 1) AAAAMM direto (D3C / D4C / D5C) ou 1994-01 / 1994.01
        for k in ("D3C","D4C","D5C"):
            v = str(item.get(k) or "")
            if len(v) == 6 and v.isdigit():
                return int(v[:4]), int(v[4:6])
            m = re.match(r"^(\d{4})[-/.](\d{2})$", v)
            if m:
                return int(m.group(1)), int(m.group(2))

        # 2) 'jan/2025' (D3N / D4N / D5N)
        for k in ("D3N","D4N","D5N"):
            v = str(item.get(k) or "").strip().lower()
            m = re.match(r"^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[/\- ](\d{4})$", v)
            if m:
                return int(m.group(2)), mes_pt[m.group(1)]

        return None, None

    # Formato API v2 (lista)
    if isinstance(payload, list) and isinstance(payload[0], dict):
        if len(payload) <= 1:
            logging.warning('Payload só com cabeçalho (len=1). Verifique parâmetros v/p na URL.');
            return pd.DataFrame(columns=['ano','mes','localidade_codigo','localidade','indice','unidade','valor'])
        # len>1: tem dados
        regs = []
        for item in payload[1:]:
            ano, mes = parse_period(item)

            v = item.get("V")
            if isinstance(v, str):
                v = v.replace(",", ".")
            try:
                valor = float(v)
            except Exception:
                valor = None

            regs.append({
                "ano": ano,
                "mes": mes,
                "localidade_codigo": item.get("D1C") or "1",
                "localidade": item.get("D1N") or "Brasil",
                "indice": "IPCA",
                "unidade": "%",
                "valor": valor,
            })

        df = pd.DataFrame(regs)
        df = df.dropna(subset=["ano","mes"])
        if not df.empty:
            df["ano"] = df["ano"].astype(int)
            df["mes"] = df["mes"].astype(int)
            df = df.sort_values(["ano","mes"]).reset_index(drop=True)
        return df[["ano","mes","localidade_codigo","localidade","indice","unidade","valor"]]

    # Fallback
    try:
        return pd.json_normalize(payload)
    except Exception:
        return pd.DataFrame()

def save_parquet(df, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", index=False)
    logging.info("Arquivo salvo: %s (linhas=%d, colunas=%d)", path, len(df), df.shape[1])

def main():
    setup_logging()
    import argparse
    p=argparse.ArgumentParser(); 
    p.add_argument("--live", action="store_true"); 
    p.add_argument("--out", type=str, default=str(OUT_DIR / "ipca.parquet"))
    args=p.parse_args()
    if args.live:
        try: payload=fetch_json_with_fallback()
        except Exception as e: logging.warning("Falha na rede: %s. Usando fixture.", e); payload=load_fixture(FIXTURE_FILE)
    else:
        payload=load_fixture(FIXTURE_FILE)
    df=normalize_ipca(payload); save_parquet(df, Path(args.out))

if __name__ == "__main__": main()

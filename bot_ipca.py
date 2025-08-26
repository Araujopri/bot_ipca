#!/usr/bin/env python3
"""
Bot de captura do IPCA (SIDRA/IBGE) e gravação em Parquet.
"""
import argparse, json, logging
from pathlib import Path
from typing import List, Dict
import pandas as pd, requests

SIDRA_URLS = [
    "https://apisidra.ibge.gov.br/values/t/1737/n1/all/p/last%20120?formato=json",
    "https://apisidra.ibge.gov.br/values/t/1737/n1/all/p/all?formato=json",
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
    if isinstance(payload, list) and len(payload)>1 and isinstance(payload[1], dict):
        regs=[]
        for item in payload[1:]:
            period=str(item.get("D3C") or "")
            ano=int(period[:4]) if period[:4].isdigit() else None
            mes=int(period[4:6]) if len(period)>=6 and period[4:6].isdigit() else None
            v=item.get("V")
            if isinstance(v,str): v=v.replace(",", ".")
            try: valor=float(v)
            except: valor=None
            regs.append({"ano":ano,"mes":mes,"localidade_codigo":item.get("D1C") or "1",
                         "localidade":item.get("D1N") or "Brasil","indice":"IPCA","unidade":"%","valor":valor})
        import pandas as pd
        df=pd.DataFrame(regs).sort_values(["ano","mes"]).reset_index(drop=True)
        return df[["ano","mes","localidade_codigo","localidade","indice","unidade","valor"]]
    import pandas as pd
    try: return pd.json_normalize(payload)
    except Exception: return pd.DataFrame()

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

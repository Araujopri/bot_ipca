# bot_ipca
Constoi um Bot que captura os dados do IPCA (no IBGE) e grava os dados em um arquivo no disco local.
[README.md](https://github.com/user-attachments/files/21994719/README.md)
# Bot IPCA (SIDRA/IBGE) -> Parquet

## Como rodar no Colab
```bash
!pip install pandas pyarrow requests
!python /content/ipca_bot/bot_ipca.py --live
```
Saídas: output/ipca.parquet, ipca_limpo.parquet, ipca_ultimos_24m.parquet, ipca_ultimos_24m.csv

"""
PIPELINE ETL — Monitoramento Climático
=============================================================
Este é o arquivo principal. Ele gerencia os três passos do pipeline:

  EXTRAÇÃO → Busca dados meteorológicos da API Open-Meteo
  TRANSFORMAÇÃO → Analisa os dados com Llama 3.3 (via Groq) e classifica o risco
  CARREGAMENTO → Salva os resultados em JSON e relatório Markdown

Como usar:
    python3 pipeline.py

Variáveis de ambiente necessárias:
    GROQ_API_KEY — sua chave de API do Groq (gratuita em https://console.groq.com)
"""

import sys, time
from datetime import datetime
from pathlib import Path

# Garante que os módulos do pacote etl sejam encontrados ao rodar da raiz do projeto
sys.path.insert(0, ".")

from etl.extrair import extrair
from etl.transformar import transformar
from etl.carregar import carregar


def executar_pipeline():
    """
    Função que executa o pipeline ETL.
    Registra o tempo total de execução e exibe um resumo ao final.
    """
    # Marca o início para calcular o tempo total ao final
    inicio = time.time()

    # Cabeçalho visual do pipeline
    print()
    print("╔══════════════════════════════════════════════╗")
    print("║        ETL — MONITORAMENTO CLIMÁTICO         ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"  Iniciado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()

    # ── EXTRAÇÃO ────────────────────────────────
    # Busca os dados brutos de temperatura, chuva, vento etc. de cada cidade
    dados_brutos = extrair()

    # Interrompe o pipeline se nenhum dado foi extraído com sucesso
    if not dados_brutos:
        print("\n❌ Nenhum dado extraído. Verifique sua conexão e tente novamente.")
        sys.exit(1)

    # ── TRANSFORMAÇÃO ───────────────────────────
    # Envia os dados ao modelo Llama e obtém recomendações
    dados_analisados = transformar(dados_brutos)

    # ── CARREGAMENTO ────────────────────────────
    # Salva os resultados em JSON estruturado e relatório Markdown legível
    caminhos_saida = carregar(dados_analisados)

    # ── RESUMO FINAL ────────────────────────────
    tempo_total = time.time() - inicio

    # Conta os alertas gerados para exibir no resumo
    total_alertas  = sum(1 for d in dados_analisados if d["nivel_risco"] == "ALERTA")
    total_atencoes = sum(1 for d in dados_analisados if d["nivel_risco"] == "ATENCAO")

    print()
    print("╔══════════════════════════════════════════════╗")
    print("║              PIPELINE CONCLUÍDO              ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"  ⏱️  Tempo total: {tempo_total:.1f}s")
    print(f"  🏙️  Cidades: {len(dados_analisados)}")
    print(f"  🔴 Em alerta: {total_alertas}")
    print(f"  🟡 Em atenção: {total_atencoes}")
    print(f"  📄 JSON:             /{Path(caminhos_saida['json']).relative_to(Path.cwd()).as_posix()}")
    print(f"  📝 Relatório MD:     /{Path(caminhos_saida['relatorio']).relative_to(Path.cwd()).as_posix()}")
    print()


# Ponto de entrada do script
if __name__ == "__main__":
    executar_pipeline()
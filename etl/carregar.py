"""
CARREGAMENTO
Salva os dados analisados em dois formatos de saída:
  - JSON estruturado em data/  → ideal para integração com sistemas ou bancos de dados
  - Relatório Markdown em reports/ → legível por humanos, pronto para publicar ou enviar
"""

import json
from datetime import datetime
from pathlib import Path


# Diretórios de saída relativos à raiz do projeto
DIRETORIO_DADOS = Path(__file__).parent.parent / "data"
DIRETORIO_RELATORIOS = Path(__file__).parent.parent / "relatorios"


def salvar_json(dados: list[dict], timestamp: str) -> Path:
    """
    Salva os dados analisados em formato JSON estruturado.
    O arquivo inclui metadados como total de alertas e horário de geração.

    Parâmetros:
        dados (list[dict]): lista de cidades com análise de risco
        timestamp (str): string de data/hora usada no nome do arquivo

    Retorna:
        Path: caminho do arquivo salvo
    """
    # Cria o diretório de saída se ainda não existir
    DIRETORIO_DADOS.mkdir(exist_ok=True)

    nome_arquivo = DIRETORIO_DADOS / f"clima_{timestamp}.json"

    # Monta o payload completo com metadados do pipeline
    payload = {
        "gerado_em": datetime.now().isoformat(),
        "total_cidades": len(dados),
        "total_alertas": sum(1 for d in dados if d["nivel_risco"] == "ALERTA"),
        "total_atencoes": sum(1 for d in dados if d["nivel_risco"] == "ATENCAO"),
        "cidades": dados,
    }

    # Salva o arquivo com indentação para facilitar leitura humana
    with open(nome_arquivo, "w", encoding="utf-8") as arquivo:
        json.dump(payload, arquivo, ensure_ascii=False, indent=2)

    return nome_arquivo


def salvar_relatorio_markdown(dados: list[dict], timestamp: str) -> Path:
    """
    Gera um relatório Markdown legível com as cidades agrupadas por nível de risco.
    Inclui dados brutos, resumo do modelo, avisos e recomendações.

    Parâmetros:
        dados (list[dict]): lista de cidades com análise de risco
        timestamp (str): string de data/hora usada no nome do arquivo

    Retorna:
        Path: caminho do arquivo salvo
    """
    DIRETORIO_RELATORIOS.mkdir(exist_ok=True)

    nome_arquivo = DIRETORIO_RELATORIOS / f"relatorio_{timestamp}.md"

    # Formata a data de geração no padrão brasileiro
    agora = datetime.now().strftime("%d/%m/%Y às %H:%M")

    # Separa as cidades por nível de risco para exibir nas seções corretas
    cidades_alerta   = [d for d in dados if d["nivel_risco"] == "ALERTA"]
    cidades_atencao  = [d for d in dados if d["nivel_risco"] == "ATENCAO"]
    cidades_normais  = [d for d in dados if d["nivel_risco"] == "NORMAL"]

    # Monta o cabeçalho do relatório com o resumo geral
    linhas = [
        "# 🌦️ Relatório de Alertas Climáticos",
        f"**Gerado em:** {agora}  ",
        f"**Cidades monitoradas:** {len(dados)}  ",
        f"**🔴 Em alerta:** {len(cidades_alerta)} | "
        f"**🟡 Em atenção:** {len(cidades_atencao)} | "
        f"**🟢 Normal:** {len(cidades_normais)}",
        "",
        "---",
        "",
    ]

    # Define as seções do relatório com seus títulos e dados correspondentes
    secoes = [
        ("🔴 ALERTA — Ação Imediata Necessária",     cidades_alerta),
        ("🟡 ATENÇÃO — Monitoramento Recomendado",   cidades_atencao),
        ("🟢 NORMAL — Condições Estáveis",           cidades_normais),
    ]

    for titulo_secao, lista_cidades in secoes:
        # Pula a seção se não houver cidades nessa categoria
        if not lista_cidades:
            continue

        linhas.append(f"## {titulo_secao}")
        linhas.append("")

        for cidade in lista_cidades:
            # Recupera os dados meteorológicos brutos preservados na transformação
            brutos = cidade.get("dados_brutos", {})

            # Bloco de informações da cidade
            linhas += [
                f"### {cidade['emoji_risco']} {cidade['cidade']} — {cidade['estado']}",
                "",
                f"**Resumo:** {cidade['resumo']}",
                "",
                "**Dados meteorológicos:**",
                f"- 🌡️ Temperatura: {brutos.get('temperatura_c', 'N/D')}°C",
                f"- 💧 Umidade: {brutos.get('umidade_pct', 'N/D')}%",
                f"- 🌧️ Precipitação: {brutos.get('precipitacao_mm', 'N/D')} mm",
                f"- 💨 Vento: {brutos.get('velocidade_vento_kmh', 'N/D')} km/h "
                f"(rajadas: {brutos.get('rajadas_vento_kmh', 'N/D')} km/h)",
                f"- 📍 Coordenadas: {brutos.get('lat', 'N/D')}° lat, {brutos.get('lon', 'N/D')}° lon",
                "",
            ]

            # Adiciona avisos apenas se houver algum
            if cidade.get("avisos"):
                linhas.append("**⚠️ Riscos identificados:**")
                for aviso in cidade["avisos"]:
                    linhas.append(f"- {aviso}")
                linhas.append("")

            # Adiciona recomendações apenas se houver alguma
            if cidade.get("recomendacoes"):
                linhas.append("**✅ Ações recomendadas:**")
                for recomendacao in cidade["recomendacoes"]:
                    linhas.append(f"- {recomendacao}")
                linhas.append("")

            linhas.append("---")
            linhas.append("")

    # Rodapé do relatório
    linhas += [
        "_Relatório gerado automaticamente via pipeline ETL com Llama 3.3 (Groq) + Open-Meteo API._",
    ]

    # Escreve o arquivo juntando todas as linhas com quebra de linha
    with open(nome_arquivo, "w", encoding="utf-8") as arquivo:
        arquivo.write("\n".join(linhas))

    return nome_arquivo


def carregar(dados: list[dict]) -> dict:
    """
    Função principal de carregamento.
    Orquestra o salvamento nos dois formatos de saída e retorna os caminhos gerados.

    Parâmetros:
        dados (list[dict]): saída da etapa de transformação

    Retorna:
        dict: dicionário com os caminhos dos arquivos gerados
    """
    print("\n" + "=" * 50)
    print("💾 CARREGAMENTO")
    print("=" * 50)

    # Gera o timestamp uma única vez para que ambos os arquivos tenham o mesmo nome base
    timestamp = datetime.now().strftime("%d-%m-%Y_%Hh%M")

    # Salva os dois formatos de saída
    caminho_json     = salvar_json(dados, timestamp)
    caminho_relatorio = salvar_relatorio_markdown(dados, timestamp)

    print(f"  ✅ JSON salvo em:        \{caminho_json.relative_to(Path.cwd())}")
    print(f"  ✅ Relatório salvo em:   \{caminho_relatorio.relative_to(Path.cwd())}")

    return {
        "json": str(caminho_json),
        "relatorio": str(caminho_relatorio),
    }
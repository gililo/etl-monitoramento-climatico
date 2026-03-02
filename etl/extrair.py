"""
EXTRAÇÃO - Lê a lista de cidades que iremos monitorar em um arquivo CSV
e busca os dados meteorológicos de cada cidade na API Open-Meteo.

Formato esperado do CSV (cidades.csv):
    nome,estado,lat,lon
    São Paulo,SP,-23.5558,-46.6396
"""

import csv, requests
from pathlib import Path

# Caminho padrão do CSV com as cidades (na raiz do projeto)
CAMINHO_CSV_PADRAO = Path(__file__).parent.parent / "cidades.csv"

# Variáveis meteorológicas que iremos solicitar à API
VARIAVEIS_METEOROLOGICAS = [
    "temperature_2m",       # Temperatura a 2 metros do solo (°C)
    "relative_humidity_2m", # Umidade relativa do ar (%)
    "precipitation",        # Precipitação acumulada (mm)
    "wind_speed_10m",       # Velocidade do vento a 10 metros (km/h)
    "wind_gusts_10m",       # Velocidade das rajadas de vento (km/h)
    "weathercode",          # Código WMO que representa a condição climática atual
]


def calcular_precipitacao_48h(dados: dict) -> float:

    # Extrai as listas de horários e precipitações do bloco "hourly" da API
    horarios = dados.get("hourly", {}).get("time", [])
    precipitacoes = dados.get("hourly", {}).get("precipitation", [])

    # Pega o horário atual do bloco "current" para saber até onde somar
    tempo_atual = dados.get("current", {}).get("time", "")

    # Se não tiver horários ou tempo atual, retorna 0 para evitar erro
    if not horarios or not tempo_atual:
        return 0.0

    try:
        # Formata o tempo atual para o padrão horário ex: "2026-03-01T19:00"
        # e encontra sua posição na lista de horários
        hora_atual = tempo_atual[:13] + ":00"
        indice_atual = horarios.index(hora_atual)
    except ValueError:
        # Se a hora atual não for encontrada na lista, usa o último horário disponível
        indice_atual = len(horarios) - 1

    # Pega todas as horas desde o início da lista até a hora atual (48h completas)
    precipitacoes_48h = precipitacoes[0: indice_atual + 1]

    return round(sum(v for v in precipitacoes_48h if v is not None), 1)


def ler_cidades_do_csv(caminho_csv: Path) -> list[dict]:
    """
    Lê o arquivo CSV e retorna a lista de cidades com suas coordenadas.
    Valida se as colunas obrigatórias estão presentes e converte
    lat/lon para float.

    Parâmetros:
        caminho_csv (Path): caminho para o arquivo CSV

    Retorna:
        list[dict]: lista de cidades com nome, estado, lat e lon

    Validação:
        FileNotFoundError: se o arquivo CSV não for encontrado
        ValueError: se alguma coluna obrigatória estiver faltando
    """
    # Verifica se o arquivo existe antes de tentar abrir
    if not caminho_csv.exists():
        raise FileNotFoundError(
            f"❌ Arquivo CSV não encontrado: {caminho_csv}\n"
            f"   Crie o arquivo com as colunas: nome, estado, lat, lon"
        )

    colunas_obrigatorias = {"nome", "estado", "lat", "lon"}
    cidades = []

    with open(caminho_csv, encoding="utf-8") as arquivo:
        leitor = csv.DictReader(arquivo)

        # Valida se todas as colunas obrigatórias estão presentes no cabeçalho
        colunas_presentes = set(leitor.fieldnames or [])
        colunas_faltando = colunas_obrigatorias - colunas_presentes
        
        if colunas_faltando:
            raise ValueError(
                f"❌ Coluna(s) faltando no CSV: {', '.join(colunas_faltando)}\n"
                f"   Colunas obrigatórias: nome, estado, lat, lon"
            )

        for numero_linha, linha in enumerate(leitor, start=2):
            # Pula linhas completamente vazias
            if not any(linha.values()):
                continue

            try:
                cidades.append({
                    "nome": linha["nome"].strip(),
                    "estado": linha["estado"].strip().upper(),
                    "lat": float(linha["lat"]),
                    "lon": float(linha["lon"]),
                })
            except ValueError:
                # Avisa sobre linhas com coordenadas inválidas mas continua lendo
                print(f"  ⚠️  Linha {numero_linha} ignorada: lat/lon inválidos "
                      f"({linha['nome']})")

    return cidades


def buscar_clima_cidade(cidade: dict) -> dict:
    """
    Faz a requisição HTTP para a API Open-Meteo e retorna os
    dados meteorológicos atuais da cidade.

    Parâmetros:
        cidade (dict): dicionário com nome, estado, lat e lon da cidade

    Retorna:
        dict: dados climáticos normalizados da cidade
    """
    url_api = "https://api.open-meteo.com/v1/forecast"

    # Monta os parâmetros da requisição
    parametros = {
        "latitude": cidade["lat"],
        "longitude": cidade["lon"],
        "current": ",".join(VARIAVEIS_METEOROLOGICAS),  # Solicita dados atuais
        "timezone": "America/Sao_Paulo",                # Ajusta o fuso horário para o Brasil
        "hourly": "precipitation",
        "forecast_days": 1,
        "past_days": 1,
    }

    # Faz a requisição com timeout de 10 segundos para evitar travamentos
    resposta = requests.get(url_api, params=parametros, timeout=10)
    # Retorna exceção se o status HTTP for erro (4xx, 5xx)
    resposta.raise_for_status() 

    dados = resposta.json()
    # Extrai apenas o bloco de dados atuais
    atual = dados.get("current", {})

    # Calcula o acumulado das últimas 48h a partir dos dados horários
    precipitacao_48h = calcular_precipitacao_48h(dados)

    # Retorna os dados mapeados
    return {
        "cidade": cidade["nome"],
        "estado": cidade["estado"],
        "lat": cidade["lat"],
        "lon": cidade["lon"],
        "horario_medicao": atual.get("time"),
        "temperatura_c": atual.get("temperature_2m"),
        "umidade_pct": atual.get("relative_humidity_2m"),
        "precipitacao_mm": atual.get("precipitation"),
        "precipitacao_48h_mm": precipitacao_48h,
        "velocidade_vento_kmh": atual.get("wind_speed_10m"),
        "rajadas_vento_kmh": atual.get("wind_gusts_10m"),
        "codigo_clima": atual.get("weathercode"),
    }


def extrair(caminho_csv: Path = CAMINHO_CSV_PADRAO) -> list[dict]:
    """
    Função principal de extração.
    Lê as cidades do CSV, busca os dados meteorológicos de cada uma
    e retorna a lista consolidada.

    Parâmetros:
        caminho_csv (Path): caminho para o CSV de cidades.
                            Se não informado, usa cidades.csv na raiz do projeto.

    Retorna:
        list[dict]: lista com os dados meteorológicos de cada cidade
    """
    print("=" * 50)
    print("📡 EXTRAÇÃO")
    print("=" * 50)

    # Lê a lista de cidades do CSV
    print(f"  📂 Lendo cidades de: /{caminho_csv.relative_to(Path.cwd())}")
    cidades = ler_cidades_do_csv(caminho_csv)
    print(f"  🏙️  {len(cidades)} cidade(s) encontrada(s) no CSV.\n")

    resultados = []

    for cidade in cidades:
        try:
            # Busca os dados meteorológicos da cidade atual na API
            dados = buscar_clima_cidade(cidade)
            resultados.append(dados)

            # Exibe um resumo rápido no terminal para acompanhar o progresso
            print(f"  ✅ {cidade['nome']} ({cidade['estado']}) — "
                  f"{dados['temperatura_c']}°C | "
                  f"{dados['precipitacao_48h_mm']}mm chuva (48h) | "
                  f"vento {dados['velocidade_vento_kmh']} km/h")

        except Exception as erro:
            # Registra o erro mas continua o pipeline para as demais cidades
            print(f"  ❌ Erro ao buscar {cidade['nome']}: {erro}")

    print(f"\n  📦 {len(resultados)} cidade(s) extraída(s) com sucesso.")
    return resultados
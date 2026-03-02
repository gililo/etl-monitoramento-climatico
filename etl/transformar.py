"""
TRANSFORMAÇÃO -  Calcula nível de risco por cidade (Normal / Atenção / Alerta) e 
envia os dados meteorológicos para o modelo Llama 3.3 (via Groq)
recebendo de volta uma análise estruturada com:
  - Resumo em linguagem natural (português)
  - Riscos identificados e ações preventivas recomendadas
"""

import os, json
from groq import Groq
from dotenv import load_dotenv

load_dotenv()


def classificar_risco(dados: dict) -> str:
    """
    Classifica o nível de risco da cidade com base nos dados meteorológicos.
    Feito em Python puro para garantir precisão.
    """
    precipitacao = dados.get("precipitacao_48h_mm", 0)
    vento = dados.get("velocidade_vento_kmh", 0)
    rajadas = dados.get("rajadas_vento_kmh", 0)
    temperatura = dados.get("temperatura_c", 20)

    if (precipitacao > 35 or vento > 60 or rajadas > 80 or
        temperatura > 40 or temperatura < 5):
        return "ALERTA"
    elif (precipitacao > 10 or vento > 40 or
          temperatura > 35 or temperatura < 10):
        return "ATENCAO"
    return "NORMAL"


# Prompt de sistema para instruir o modelo sobre seu papel e os critérios de classificação
PROMPT_SISTEMA = """
Você é um sistema especialista em alertas climáticos para municípios brasileiros.
Receberá dados meteorológicos brutos de cidades e deverá analisar cada uma delas.

Para cada cidade, retorne um objeto JSON com os seguintes campos:
- "cidade": nome da cidade
- "estado": estado (sigla)
- "nivel_risco": repita exatamente o valor recebido, sem alterar
- "resumo": crie um resumo personalizado para cada cidade de acordo com os
            parametros obtidos da situação climática atual de cada cidade
            em no maximo 100 caracteres, e no minimo 5 palavras, em português.
            Cada cidade tem dados diferentes — o resumo deve refletir frases
            diferentes.
- "avisos": lista de strings com riscos identificados (pode ser vazia)
- "recomendacoes": lista de strings com ações preventivas recomendadas

Retorne SOMENTE um array JSON válido, sem texto adicional e sem blocos de markdown.
"""


def montar_mensagem_usuario(dados_climaticos: list[dict]) -> str:
    """
    Monta a mensagem que será enviada ao modelo com os dados brutos das cidades.

    Parâmetros:
        dados_climaticos (list[dict]): lista de dados meteorológicos extraídos

    Retorna:
        str: mensagem formatada para o modelo
    """

        # Adiciona o nivel_risco calculado em cada cidade antes de enviar ao modelo
    dados_com_risco = []
    for cidade in dados_climaticos:
        copia = dict(cidade)
        copia["nivel_risco"] = classificar_risco(cidade)
        dados_com_risco.append(copia)

    return (
        "Gere o resumo e recomendações para cada cidade:\n\n"
        + json.dumps(dados_com_risco, ensure_ascii=False, indent=2)
    )


def limpar_resposta_json(texto: str) -> str:
    """
    Remove blocos de markdown que o modelo pode gerar
    na resposta, garantindo que apenas o JSON puro seja processado.

    Parâmetros:
        texto (str): resposta bruta do modelo

    Retorna:
        str: JSON limpo, pronto para parsing
    """
    if texto.startswith("```"):
        # Remove a primeira linha (```json ou ```) e a última (```)
        partes = texto.split("```")
        texto = partes[1]
        if texto.startswith("json"):
            texto = texto[4:]  # Remove o identificador de linguagem
    return texto.strip()


def transformar(dados_climaticos: list[dict]) -> list[dict]:
    """
    Função principal de transformação.
    Envia os dados ao modelo Llama e retorna a análise enriquecida.

    Parâmetros:
        dados_climaticos (list[dict]): saída da etapa de extração

    Retorna:
        list[dict]: dados analisados com nível de risco, resumo e recomendações
    """
    print("\n" + "=" * 50)
    print("🤖 TRANSFORMAÇÃO (Llama via Groq)")
    print("=" * 50)

    # Verifica se a chave de API foi configurada no ambiente
    chave_api = os.environ.get("GROQ_API_KEY")
    if not chave_api:
        raise EnvironmentError(
            "❌ Variável de ambiente GROQ_API_KEY não definida.\n"
            "   Crie sua chave gratuita em https://console.groq.com"
        )

    # Inicializa o cliente Groq com a chave de API
    cliente = Groq(api_key=chave_api)

    print("  ⏳ Enviando dados para análise pelo modelo Llama 3.3...")

    # Faz a chamada ao modelo com temperatura baixa para respostas mais determinísticas
    resposta = cliente.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": PROMPT_SISTEMA},
            {"role": "user",   "content": montar_mensagem_usuario(dados_climaticos)},
        ],
        temperature=0.2,   # Baixo: respostas mais consistentes e objetivas
        max_tokens=2048,   # Limite suficiente para analisar todas as cidades
    )

    # Extrai o texto da resposta
    texto_resposta = resposta.choices[0].message.content.strip()

    # Limpa possível formatação markdown e faz o parsing do JSON
    json_limpo = limpar_resposta_json(texto_resposta)
    dados_analisados = json.loads(json_limpo)

    # Cria um índice dos dados brutos por cidade para mesclar depois
    dados_brutos_por_cidade = {item["cidade"]: item for item in dados_climaticos}

    # Mescla os dados brutos com a análise do modelo para preservar os valores originais
    for item in dados_analisados:
        item["dados_brutos"] = dados_brutos_por_cidade.get(item["cidade"], {})

    # Sobrescreve o nivel_risco e emoji com o valor calculado em Python — ignora o do Llama
    emojis = {"NORMAL": "🟢", "ATENCAO": "🟡", "ALERTA": "🔴"}
    for item in dados_analisados:
        item["nivel_risco"] = classificar_risco(dados_brutos_por_cidade.get(item["cidade"], {}))
        item["emoji_risco"] = emojis[item["nivel_risco"]]

    # Exibe o resultado resumido de cada cidade no terminal
    for item in dados_analisados:
        print(f"  {item['emoji_risco']} {item['cidade']} ({item['estado']}) — {item['nivel_risco']}")
        if item.get("avisos"):
            for aviso in item["avisos"]:
                print(f"      ⚠️  {aviso}")

    print(f"\n  ✅ Transformação concluída para {len(dados_analisados)} cidade(s).")
    return dados_analisados
# 🌦️ ETL — Monitoramento Climático por Cidade
Lab Project - Explorando IA Generativa em um Pipeline de ETL com Python - DIO

Pipeline ETL que monitora condições climáticas em cidades brasileiras, analisa os dados com **Llama 3.3** (via Groq) e gera relatórios automáticos de alerta em português.

> ⚠️ **Aviso importante:** Este projeto foi desenvolvido para fins **didáticos**. Os limiares de classificação de risco (temperatura, chuva, vento) não foram definidos por um profissional de meteorologia ou defesa civil. 


## 🏗️ Arquitetura do Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                         PIPELINE ETL                         │
│                                                              │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐     │
│   │   EXTRAÇÃO   │ ─>│ TRANSFORMAÇÃO│ ─>│ CARREGAMENTO │     │
│   │ cidades.csv  │   │              │   │              │     │
│   │      +       │   │  Llama 3.3   │   │ JSON + MD    │     │
│   │ Open-Meteo   │   |  via Groq    │   │ relatórios   │     │
│   │ API (grátis) │   │ (grátis)     │   │              │     │
│   └──────────────┘   └──────────────┘   └──────────────┘     │
│   temperatura, chuva  resumo e          data/ relatorios/    │
│   vento, umidade      recomendações     prontos p/ envio     │
└──────────────────────────────────────────────────────────────┘
```

### 📡 Extração (`etl/extrair.py`)
Lê as cidades do `cidades.csv` e busca os dados atuais de cada uma na [Open-Meteo API](https://open-meteo.com/) (gratuita, sem cadastro). Além dos dados instantâneos, calcula a **precipitação acumulada nas últimas 48 horas**, que é muito mais representativa para avaliar risco de chuva do que o valor pontual atual.

### 🤖 Transformação (`etl/transformar.py`)
A classificação de risco é feita em **Python puro**, com critérios numéricos fixos — garantindo precisão e consistência. O **Llama 3.3 70B** via [Groq](https://console.groq.com) é usado apenas para gerar o resumo em linguagem natural e as recomendações para a população, que é onde a IA realmente agrega valor.

### 💾 Carregamento (`etl/carregar.py`)
Salva um arquivo **JSON** estruturado em `data/` e gera um **relatório Markdown** em `relatorios/`, com as cidades agrupadas por nível de risco e prontas para publicar ou enviar.

---

## ⚠️ Limitações Conhecidas

**Fonte de dados:** A [Open-Meteo](https://open-meteo.com/) é baseada em modelos de previsão numérica. O INMET utiliza estações físicas no solo, que são mais precisas para eventos climáticos locais e intensos. Por isso, pode haver divergência entre os alertas gerados por este pipeline e os alertas oficiais.

**Precipitação acumulada:** O pipeline usa as últimas 48 horas de dados horários para calcular a chuva acumulada. Eventos muito intensos e pontuais podem não ser capturados corretamente dependendo do horário de execução.

**Limiares de risco:** Os valores abaixo foram definidos para fins didáticos e não representam critérios oficiais de meteorologia ou defesa civil:

| Nível | Precipitação (48h) | Vento | Temperatura |
|---|---|---|---|
| 🔴 ALERTA | > 35mm | > 60 km/h | > 40°C ou < 5°C |
| 🟡 ATENÇÃO | > 10mm | > 40 km/h | > 35°C ou < 10°C |
| 🟢 NORMAL | demais condições | — | — |

---

## 🚀 Como Usar

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/etl-monitoramento-climatico.git
cd etl-monitoramento-climatico
```

### 2. Instale as dependências
```bash
pip install -r requirements.txt
```

### 3. Configure sua chave de API do Groq
Crie uma conta gratuita em [console.groq.com](https://console.groq.com), gere uma chave e configure:

```bash
cp .env.example .env
# edite o .env e adicione sua GROQ_API_KEY
```

### 4. Configure as cidades no CSV
Edite o `cidades.csv` com as cidades que deseja monitorar:

```csv
nome,estado,lat,lon
Manaus,AM,-3.1316,-59.5857
Belem,PA,-1.455,-48.502
Recife,PE,-8.03,-34.54
```

Para encontrar as coordenadas de qualquer cidade, use o [Google Maps](https://maps.google.com) — clique com o botão direito no local desejado.

### 5. Execute o pipeline
```bash
python3 pipeline.py
```

Ou passando um CSV personalizado:
```bash
python3 pipeline.py --cidades outro_arquivo.csv
```

## 📁 Estrutura do Projeto

```
etl-monitoramento-climatico/
│
├── pipeline.py            # Orquestrador principal — rode este!
│
├── etl/
│   ├── extrair.py         # Passo 1: le cidades.csv e busca dados da API Open-Meteo
│   ├── transformar.py     # Passo 2: classifica risco + gera resumo com Llama
│   └── carregar.py        # Passo 3: salva JSON + relatório Markdown
│
├── cidades.csv            # Lista de cidades a monitorar — edite conforme necessário
├── data/                  # JSONs gerados (gitignored)
├── relatorios/            # Relatórios .md gerados (gitignored)
│
├── .env
├── .gitignore
└── README.md
```

## 🔧 Tecnologias Utilizadas

| Tecnologia | Uso | Custo |
|---|---|---|
| [Open-Meteo](https://open-meteo.com/) | API meteorológica | Gratuito |
| [Groq](https://console.groq.com) | Inferência do modelo | Gratuito (tier gratuito) |
| [Llama 3.3 70B](https://groq.com) | Resumo e recomendações em português | Gratuito via Groq |
| Python + requests | Requisições HTTP | — |
| python-dotenv | Carregamento de variáveis de ambiente | — |


## 📄 Licença

Este projeto está licenciado sob [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).
Uso pessoal e educacional liberado. Uso comercial não permitido.

# CRM Integration Pipeline & Gold Layer

Sistema centralizado de CRM projetado para extrair, consolidar e processar dados de marketing da Hotmart e ManyChat. O projeto foca em construir uma "Fonte Única da Verdade" (Single Source of Truth) para automação de remarketing e análise de audiência.

## Como Executar

O projeto utiliza `uv` para gerenciamento de dependências e ambiente virtual.

### 1. Configuração Inicial
Crie um arquivo `.env` na raiz (use o `.env.example` como base):
```bash
ENVIRONMENT=dev # dev, hml ou prd
HOTMART_CLIENT_ID=...
HOTMART_CLIENT_SECRET=...
HOTMART_BASIC_TOKEN=...
```

### 2. Rodando a Pipeline (Manual)
Para rodar toda a pipeline imediatamente (Sync Hotmart -> Import ManyChat -> Gold Audiences -> Remarketing):
```bash
# O sistema criará as pastas data/db/{env}/ automaticamente
ENVIRONMENT=dev uv run python src/orchestrator.py --now
```

### 3. Rodando em Modo Homologação (HML)
O modo HML busca automaticamente os dados do mês passado e aguarda 10 segundos para iniciar:
```bash
ENVIRONMENT=hml uv run python src/orchestrator.py --now
```

### 4. Servidor de Agendamento
Para deixar o processo rodando conforme o horário definido no `Config` (ou `.env` via `SCHEDULE_TIME`):
```bash
uv run python src/orchestrator.py
```

---

## Arquitetura de Dados (MDM)

O sistema segue uma estrutura de camadas para garantir resiliência e clareza:

### Camada Raw/Trusted (Bronze/Silver)
- **Hotmart Sync**: Extração via API com suporte a paginação e histórico completo.
- **ManyChat Import**: Processamento de CSVs manuais colocados em `data/input/manychat/`.
- **Consolidação Master**: Fusão de dados priorizando Hotmart sobre ManyChat, limpando duplicidade e normalizando telefones/emails.

### Camada Gold (Business Intelligence)
- **Audiências**: Tabelas de público segmentadas (`audience_ilpi`, `audience_estetica`) com cálculo de **LTV (Lifetime Value)**.
- **Remarketing**: Geração de lotes diários de até 50 contatos elegíveis, respeitando janelas de 30 dias após a última compra ou último contato.

---

## Qualidade e Testes

O projeto utiliza técnicas avançadas de engenharia de software:
- **Testes MC/DC**: Garantia de cobertura total de caminhos lógicos no mapeamento.
- **Property-Based Testing (Hypothesis)**: Validação de resiliência contra dados malformados.
- **Automação de Commit**: Script `./scripts/commit.sh "msg"` que roda Lint (Ruff), Formatação (Black) e Testes (Pytest) antes de permitir o commit.

---

## Tecnologias
- **Linguagem**: Python 3.10+
- **Database**: SQLite (Isolamento por ambiente: `dev`, `hml`, `prd`)
- **Gestão**: `uv` (Fast Python package installer)
- **Linter/Formatter**: Ruff & Black

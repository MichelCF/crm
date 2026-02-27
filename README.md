# Integração CRM

Um projeto de integração centralizada de CRM que extrai dados de múltiplas fontes de marketing — atualmente **Hotmart** (Vendas e Transações) e **ManyChat** (Interações de Leads) — para um banco de dados SQLite local unificado.

## Arquitetura (V1)

O sistema conta com uma **Arquitetura de Banco de Dados em 3 Camadas** para garantir a integridade dos dados:
1. `hotmart_customers` (Bruta): Armazena os perfis de compradores inalterados da API da Hotmart.
2. `manychat_contacts` (Bruta): Armazena um histórico de atributos de leads exatamente como exportado dos CSVs do ManyChat.
3. `customers` (Mestra): A "Fonte Única da Verdade" (Single Source of Truth). Esta tabela funde (merge) automaticamente os registros das tabelas brutas usando **E-mail ou WhatsApp** como chave de identificação primária. Ela ignora ativamente contatos que não possuam nenhum dos dois métodos de contato para manter o banco de dados limpo.

## Pipelines Incluídos na V1

### 1. Integração API Hotmart
- **Sincronização Inicial e Incremental:** Busca de forma inteligente todo o histórico de dados ou apenas as novidades desde a última execução.
- **Fatiamento de Datas:** Evita os limites de erro 400 da API da Hotmart quebrando automaticamente grandes períodos (> 2 anos) em lotes gerenciáveis.
- **Todos os Status:** Captura todos os status de transação (inclindo Carrinhos Abandonados, Chargebacks, Cancelados) para gatilhos robustos de remarketing.
- **Uso:**
  ```bash
  uv run src/pipelines/hotmart_to_db.py
  ```

### 2. Importador CSV ManyChat
- **Padronização de Dados:** Lê exportações do ManyChat em Português, corrigindo notações decimais com vírgula e convertendo datas seriais do Excel (`46057,56185`) para o formato padrão ISO (`YYYY-MM-DD`).
- **Motor de Fusão Mestre:** Atualiza os registros de Clientes Mestres existentes ou cria novos com base nas informações de contato presentes no arquivo.
- **Uso:**
  Basta colocar o arquivo `manychat_output.csv` na pasta `data/` e rodar:
  ```bash
  uv run src/pipelines/manychat_csv_importer.py
  ```

## Próximos Passos (V2)
- **Extração Automática do ManyChat:** Transição de downloads manuais de CSV para um pipeline automatizado (ex: lendo automaticamente do Google Sheets sincronizado com o ManyChat).
- **Agendamento Diário:** Estabelecer uma rotina (cron job / tarefa agendada) para executar todo o pipeline (Extrações Hotmart e ManyChat) diariamente.
- **Integração Outbound via API:** Enviar proativamente classificações do CRM (como "Comprador" ou "Carrinho Abandonado") de volta para o ManyChat através da sua API.

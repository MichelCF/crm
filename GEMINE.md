# Projeto CRM

**Objetivo:** Desenvolver um CRM personalizado. Esse CRM terá 3 grandes fontes de dados: arquivos CSV, contatos do ManyChat e clientes da Hotmart.

## Metas Principais
1. **Unificação:** Unificar os clientes com suas devidas marcações (tags).
2. **Integração com ManyChat:** Manipular o ManyChat via API para gerenciar tags, realizar exclusões e inclusões de contatos, adicioná-los a listas de disparo, padronizar tags, etc.
3. **Métricas de Tráfego:** Conectar às plataformas Meta Ads e Google Ads para obter uma visão dos custos de tráfego e calcular corretamente as métricas de leads e marketing.

## Stack Tecnológico
**Ferramentas que vamos usar:** 
- Python 3.10
- `pytest`
- `requests`
- `pandas`
- SQLite
- `uv` (Gerenciador de pacotes)

## Diretrizes de Desenvolvimento
- Utilizaremos **TDD** (Test-Driven Development) neste projeto.
- Faremos uso intenso de **Git** e **GitHub Actions** para testes e deploy contínuos.
- **Fluxo de Trabalho Git:**
  - O desenvolvimento padrão não ocorre na branch `main`.
  - Usaremos a estratégia: **`branch da tarefa`** -> **`dev`** -> **`main`**.
  - Como é um projeto solo, as pequenas features podem ir direto da `branch da tarefa` para a `dev`. Estando estável na `dev`, unificamos com a `main`.
- Por se tratar de um projeto **open-source**, a segurança das credenciais será uma prioridade desde o momento zero.
- No primeiro momento, o foco não será o desenvolvimento de um front-end, mas sim a criação e orquestração de **scripts** eficientes.

---

> **Nota:** O objetivo deste arquivo é servir como um onboarding para o agente de IA.

## Regras de Execução para a IA
- O agente **DEVE executar comandos no terminal** (como scripts, verificações ou comandos de git locais) sempre que necessário para ler os logs automaticamente e não depender de copiar/colar.
- **NUNCA execute `git push`**: O agente é expressamente proibido de rodar comandos de push para o repositório remoto. O *push* deve ser sempre feito manualmente pelo usuário.
- **Atenção ao `uv` e binários locais:** Como o ambiente do agente pode não carregar o `$PATH` do usuário (`~/.cargo/bin` ou `~/.local/bin`), ao rodar o `uv`, deve-se usar o caminho absoluto (ex: `~/.cargo/bin/uv`) ou carregar o path na mesma linha antes do comando.
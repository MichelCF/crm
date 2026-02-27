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
- Por se tratar de um projeto **open-source**, a segurança das credenciais será uma prioridade desde o momento zero.
- No primeiro momento, o foco não será o desenvolvimento de um front-end, mas sim a criação e orquestração de **scripts** eficientes.

---

> **Nota:** O objetivo deste arquivo é servir como um onboarding para o agente de IA.

## Regras de Execução para a IA
- O agente **NÃO deve executar comandos no terminal** (especialmente comandos `uv`, `git` ou de instalação/execução). 
- Em vez disso, o agente deve **apenas fornecer os comandos no chat** para que o próprio usuário os execute manualmente.
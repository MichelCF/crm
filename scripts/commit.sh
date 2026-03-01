#!/bin/bash

# Check if commit message is provided
if [ -z "$1" ]; then
    echo "Erro: Por favor, forneça uma mensagem de commit."
    echo "Uso: ./scripts/commit.sh \"mensagem de commit\""
    exit 1
fi

COMMIT_MSG="$1"

echo "Iniciando automação de commit..."

echo "Passo 1: Rodando Ruff (Lint/Fix)..."
~/.local/bin/uv run ruff check . --fix
if [ $? -ne 0 ]; then
    echo "Erro: Ruff encontrou problemas. Abortando commit."
    exit 1
fi

echo "Passo 2: Rodando Black (Formatação)..."
~/.local/bin/uv run black .
if [ $? -ne 0 ]; then
    echo "Erro: Black falhou. Abortando commit."
    exit 1
fi

echo "Passo 3: Rodando Pytest (Testes)..."
~/.local/bin/uv run pytest tests/ -v
if [ $? -ne 0 ]; then
    echo "Erro: Os testes falharam. Abortando commit."
    exit 1
fi

echo "Passo 4: Tudo pronto! Realizando git add e commit..."
git add .
git commit -m "$COMMIT_MSG"

echo "Sucesso! Código limpo e comitado."

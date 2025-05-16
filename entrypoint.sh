#!/bin/sh

if [ -z "$SCRIPT_TO_RUN" ]; then
  echo "Erro: defina SCRIPT_TO_RUN (ex: logbook_pyspark.py)"
  exit 1
fi

# Comando padrão: python
COMMAND=${RUN_COMMAND:-python}

echo "Executando: $COMMAND $SCRIPT_TO_RUN"

if [ "$COMMAND" = "pytest" ]; then
  exec python -m pytest "$SCRIPT_TO_RUN"
else
  exec python "$SCRIPT_TO_RUN"
fi

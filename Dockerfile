#imagem base com Python e Spark já instalados
FROM bitnami/spark:latest

# Instala o pandas (se precisar) e outras dependências
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir pyarrow
RUN pip install --no-cache-dir pytest

# Set working directory dentro do container
WORKDIR /app

# Copia os arquivos do projeto para o container
COPY . /app

USER root
RUN chmod -R 777 /app

# Comando para rodar seu script Python
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["diario_de_bordo/pyspark_solution/logbook_pyspark.py"]

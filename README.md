# 🚗 logbook-f1rst-challenge

Projeto de engenharia de dados para transformar registros brutos de corridas de transporte privado em uma tabela limpa e agregada por data, utilizando **Pandas** e **PySpark**. A nova tabela gerada se chama `info_corridas_do_dia`.

---

## 📌 Objetivo

Criar uma nova tabela chamada `info_corridas_do_dia`, agrupando os dados pela data de início do transporte (`DT_REFE`) com formatação `yyyy-MM-dd` e contendo as colunas descritas abaixo:

| Coluna              | Descrição                                                                 |
|---------------------|---------------------------------------------------------------------------|
| `DT_REFE`           | Data de referência.                                                       |
| `QT_CORR`           | Quantidade total de corridas.                                             |
| `QT_CORR_NEG`       | Quantidade de corridas com a categoria "Negócio".                         |
| `QT_CORR_PESS`      | Quantidade de corridas com a categoria "Pessoal".                         |
| `VL_MAX_DIST`       | Maior distância percorrida por uma corrida.                               |
| `VL_MIN_DIST`       | Menor distância percorrida por uma corrida.                               |
| `VL_AVG_DIST`       | Média das distâncias percorridas.                                         |
| `QT_CORR_REUNI`     | Quantidade de corridas com o propósito de "Reunião".                      |
| `QT_CORR_NAO_REUNI` | Quantidade de corridas com propósito declarado diferente de "Reunião".    |

---

## 🛠️ Tecnologias utilizadas

- Python
- Pandas
- PySpark
- Pytest
- Docker

---

## 📂 Estrutura de entrada e saída

- **Entrada**: Arquivo CSV com os dados brutos das corridas.
- **Saída**: 
  - Arquivo `.parquet` com os dados transformados.
  - Arquivo `.csv` com os mesmos dados agregados.

---

## 🐳 Como executar com Docker

### 1. Construir a imagem Docker

```bash
docker build -t logbook-spark .
```

### 2. Executar o script com **Pandas**

```bash
docker run --rm   -v < seu_path >:/app/diario_de_bordo/results   -e SCRIPT_TO_RUN=diario_de_bordo/pandas_solution/logbook_pd.py   -e RUN_COMMAND=python   logbook-spark
```

### 3. Executar o script com **PySpark**

```bash
docker run --rm   -v < seu path >:/app/diario_de_bordo/dados   -e SCRIPT_TO_RUN=diario_de_bordo/pyspark_solution/logbook_pyspark.py   -e RUN_COMMAND=python   logbook-spark
```

### 4. Executar os testes com **Pytest**

```bash
docker run --rm   -v < seu path >:/app/diario_de_bordo/dados   -e SCRIPT_TO_RUN=diario_de_bordo/tests/test_logbook_pyspark.py   -e RUN_COMMAND=pytest   logbook-spark

docker run --rm   -v < seu path >:/app/diario_de_bordo/dados   -e SCRIPT_TO_RUN=diario_de_bordo/tests/test_logbook_pd.py   -e RUN_COMMAND=pytest   logbook-spark
```

## ✅ Testes

Os testes foram implementados com `pytest` e verificam a consistência dos dados gerados, incluindo contagem correta, tratamento de valores nulos e conformidade com as regras de agregação.

---

## 📈 Resultado final

A tabela `info_corridas_do_dia` é gerada de forma automatizada, garantindo dados prontos para consumo analítico com qualidade e padronização.

---

## 👩‍💻 Autora

Ana Paula Mamprin  
Email: [ana.mamprin@hotmail.com](mailto:ana.mamprin@hotmail.com)

---

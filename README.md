# KAGGLE-API-SPARK

## Introduction

This project consists on an ETL where Kaggle API datasets are downloaded and processed with PySpark and Python in a modular environment. 

## Table of Contents

- [KAGGLE-API-SPARK](#kaggle-api-spark)
  - [Introduction](#introduction)
  - [Table of Contents](#table-of-contents)
  - [Project structure](#project-structure)
    - [Folder explanation](#folder-explanation)

## Project structure

```bash
Project/
│
├── assets/
│   └── erd/
|
├── libs/
│   └── postgresql-<version>.jar  
│ 
├── modules/
│   └── utils.zip ## Use with spark-submit "--py-files" ##
│
├── src/
│   ├── mock/
|   |   └── main.py
│   └── netflix/
│       ├── main.py
|       └── main_sql.py ## Use Spark SQL ##
|
├── test/
│   ├── kaggle_test.py
|   └── postgre_staging_test.py
|
├── utils/
│   ├── config.py
│   ├── etl_functions.py
│   ├── regex_functions.py
│   ├── singletons.py
│   ├── spark_logger.py
│   └── spark_udfs.py
|
│
├── .env ## Configure this previous to work with the project ##
|
├── .gitignore
|
├── database.ini ## Main use for PostgreSQL, configure it previous to work with the project ##
│
└── README.md
```

### Folder explanation

- **assets**: contains all files like CSV, JSON...
  - **erd**: ERD (Entity Relationship Diagram) dedicated folder
    - If working with VS Code, use ERD Editor extension
- **libs**: Main folder for JAR files
- **modules**: Use this folder if you work via spark-submit, so Spark can use utils functions
- **src**: Source folder for all different processing uses
- **test**: Testing folder
- **utils**: Main folder for every and any functionality the project would need 
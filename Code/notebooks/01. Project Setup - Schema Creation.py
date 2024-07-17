# Databricks notebook source
# MAGIC %md
# MAGIC ###  Storage Path Extraction Urls for bronze, silver and gold 

# COMMAND ----------

# MAGIC %run "/Workspace/Project/04. Common"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Text widget for dynamic eniviroment extraction

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions Overview
# MAGIC
# MAGIC - **create_Bronze_Schema(environment, path):** This function sets the current catalog to the specified environment's bronze catalog and creates a bronze schema at the given path if it does not already exist.
# MAGIC
# MAGIC - **create_Silver_Schema(environment, path):** Similar to the bronze schema function, this sets the current catalog to the specified environment's silver catalog and creates a silver schema at the given path if it does not already exist.
# MAGIC
# MAGIC - **create_Gold_Schema(environment, path):** Similar to the bronze schema function, this sets the current catalog to the specified environment's gold catalog and creates a gold schema at the given path if it does not already exist.

# COMMAND ----------

def create_Bronze_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Bronze Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `bronze` MANAGED LOCATION '{path}/bronze'""")

# COMMAND ----------

def create_Silver_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Silver Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{path}/silver'""")

# COMMAND ----------

def create_Gold_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Gold Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{path}/gold'""")

# COMMAND ----------

create_Bronze_Schema(env,bronze)

# COMMAND ----------

create_Silver_Schema(env,silver)

# COMMAND ----------

create_Gold_Schema(env,gold)
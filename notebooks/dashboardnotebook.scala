// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC create database if not exists dashboarddb

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create table if not exists dashboarddb.customers
// MAGIC using csv
// MAGIC options
// MAGIC (
// MAGIC path "/mnt/data/customers/*.csv",
// MAGIC inferSchema True,
// MAGIC header True,
// MAGIC sep ","
// MAGIC )

// COMMAND ----------

val getCustomerType = (credit: Int) => {
  if(credit >= 1 && credit < 1000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC 
// MAGIC CREATE WIDGET DROPDOWN customertype DEFAULT "Gold" 
// MAGIC CHOICES 
// MAGIC SELECT DISTINCT getCustomerType(credit) customertype 
// MAGIC FROM dashboarddb.customers

// COMMAND ----------

val selectedCustomerType = dbutils.widgets.get("customertype")
val resultsv2 = spark.sql("SELECT customerid, fullname, address, credit, getCustomerType(credit) as customertype, status FROM dashboarddb.customers WHERE getCustomerType(credit) = '" + selectedCustomerType + "'")

display(resultsv2)

// COMMAND ----------

// MAGIC %md 
// MAGIC Learn ADM
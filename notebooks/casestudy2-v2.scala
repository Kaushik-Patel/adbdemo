// Databricks notebook source
import spark.implicits._

case class Customer(customerid: Int, fullname: String, address: String, credit: Int, status: Boolean)
case class Order(orderid: Int, orderdate: String, customer: Int, product:Int, units: Int, billingaddress: String,remarks: String)
case class Product(productid: BigInt, title: String, unitsinstock: BigInt, unitprice: BigInt, itemdiscount: BigInt)


// COMMAND ----------

val customers = spark.read.option("header", true).option("inferSchema", true).option("sep", ",").csv("/mnt/data/customers/*.csv").as[Customer]
val products = spark.read.option("multiline", true).json("/mnt/data/products/*.json").as[Product]
val orders = spark.read.option("inferSchema", true).option("header", true).option("sep", ",").csv("/mnt/data/orders/*.csv").as[Order]


// COMMAND ----------

customers.createOrReplaceTempView("customers")
products.createOrReplaceTempView("products")
orders.createOrReplaceTempView("orders")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Orders

// COMMAND ----------

val getCustomerType = (credit: Int) => {
  if(credit >= 1 && credit < 1000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT o.orderid, o.orderdate, c.fullname, c.address, getCustomerType(c.credit) as customertype, c.status,
// MAGIC   p.title as ProductTitle, p.unitprice * o.units as orderamount, p.unitprice,
// MAGIC   ((p.unitprice * o.units) * p.itemdiscount * 0.01) as discountamount,
// MAGIC   o.orderdate, o.billingaddress, o.units, o.remarks
// MAGIC FROM orders o
// MAGIC INNER JOIN customers c on o.customer = c.customerid
// MAGIC INNER JOIN products p on o.product = p.productid

// COMMAND ----------

val statement = """SELECT o.orderid, o.orderdate, c.fullname, c.address, getCustomerType(c.credit) as customertype, c.status,
  p.title as ProductTitle, p.unitprice * o.units as orderamount, p.unitprice,
  ((p.unitprice * o.units) * p.itemdiscount * 0.01) as discountamount,
  o.billingaddress, o.units, o.remarks
FROM orders o
INNER JOIN customers c on o.customer = c.customerid
INNER JOIN products p on o.product = p.productid"""

val results = spark.sql(statement)




// COMMAND ----------

results.write.parquet("/mnt/data/optimized-orders-store/data.parquet")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS CaseStudyDB

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS CaseStudyDB.ProcessedOrders
// MAGIC USING PARQUET
// MAGIC OPTIONS
// MAGIC (
// MAGIC   path "/mnt/data/optimized-orders-store/data.parquet"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT sum(orderamount), billingaddress
// MAGIC FROM CaseStudyDB.ProcessedOrders
// MAGIC GROUP BY billingaddress

// COMMAND ----------

results.printSchema

// COMMAND ----------

val blobStorage = "polystorageacc.blob.core.windows.net"
val blobContainer = "polystorage"
val blobAccessKey =  "6jrrfYbUv9XViu1jgqEp7N3Ro39voDVzQIyYMsm6iUTdYhRXjZfHByV9Lt3gGdE6LGqS2HL/mM8+TOfUMM+jCQ=="

val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"
val acntInfo = "fs.azure.account.key."+ blobStorage

sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

val dwDatabase = "snapwarehouse"
val dwServer = "snapsqlserver.database.windows.net"
val dwUser = "kapatel"
val dwPass = "1qaz!QAZ"
val dwJdbcPort =  "1433"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass


// COMMAND ----------

spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")

// COMMAND ----------

results
	.write
	.format("com.databricks.spark.sqldw")
	.option("url", sqlDwUrlSmall)
	.option("dbtable", "ProcessedOrders")       
	.option( "forward_spark_azure_storage_credentials","True")
	.option("tempdir", tempDir)
	.mode("overwrite")
	.save()
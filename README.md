# Analyzing Amazon reviews

For this project one of the paid Amazon Vine program database is chosen. Each one contains reviews of a specific product, from clothing apparel to wireless products. the below database is chosen for this project;
	 https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv.gz
The purpose of this project is to use PySpark to perform the ETL process to extract the dataset, transform the data, connect to an AWS RDS instance, and load the transformed data into pgAdmin.

## Perform ETL on Amazon Product Reviews

In this phase, AWS RDS database is created and also cloud-based notebooks(Google colab) is used.

firstly, the database is extracted into a DataFrame with the below code;

	 - url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.option("encoding", "UTF-8").csv(SparkFiles.get(""), sep="\t", header=True, inferSchema=True)
df.show()

The below tables are prepared:
 - customers_table  
 - products_table 
 - review_id 
 - vine_table
 
 # customers_table
	 - customers_df = df.groupby("customer_id").agg({"*": "count"}).withColumnRenamed("count(1)", "customer_count")
customers_df.show()

# products_table 

	 -products_df = df.select(["product_id","product_title"]).drop_duplicates()
products_df.show()

# review_id
	 review_id_df = df.select(["review_id","customer_id","product_id","product_parent", to_date("review_date", 'yyyy-MM-dd').alias("review_date")])
review_id_df.show()

#  vine_table

	 - vine_df = df.select(["review_id","star_rating","helpful_votes","total_votes","vine","verified_purchase"])
	 vine_df.show()

Then we link AWS RDS with Pgadmin (one server is created and connect it to RDS) and then above tables are created in pgadmin.

Next, the created tables/DataFrame in google colab are improt to Pd admin with the below codes:

	 mode = "overwrite"
	 jdbc_url="jdbc:postgresql://<RDS link>:5432/<database name>"
	 config = {"user":"****", 
	 "password": "****",
	 "driver":"org.postgresql.Driver"}

	 review_id_df.write.jdbc(url=jdbc_url, table='review_id_table', mode=mode, properties=config)
	 products_df.write.jdbc(url=jdbc_url, table='products_table', mode=mode, properties=config)
	 customers_df.write.jdbc(url=jdbc_url, table='customers_table', mode=mode, properties=config)
	 vine_df.write.jdbc(url=jdbc_url, table='vine_table', mode=mode, properties=config)


## Determine Bias of Vine Reviews

In this phase. You’ determine if there is any bias towards reviews that were written as part of the Vine program. For this analysis, we determine if having a paid Vine review makes a difference in the percentage of 5-star reviews.

Vine table is analyzed in this phase. for filtering the table .filter() library is used and also for grouping and count each group groupby() and count() are used together. 

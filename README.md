# Statsbomb-project
Applying data engineering process to the statsbomb open football data.
The project comprises of:

### **Capstone-Project-Template** file  the main program where it starts with:
* Applying the extaction / collection of data  
* Dumping the collected data directly into **AWS S3 buckets** instead of downloading it to disk .
* Transformation of the data using PySpark takes place by reading the data placed in the **AWS S3 buckets** .
* Then Copying/Loading the data into the **AWS Redshift** .

Other subsidary files completes the ETL pipeline as follwing :
1) *sql_queries_statsbomb* file conatins all the queries of our project , these queries drop , creates , inserts and copies the data.
2) *create_tbls_statsbomb* file executes the dropping and creating queries for our tables.
3) *etl_statsbomb-checkpoint* file executes the queries that do the copying and inserting of the data into the tables.
4) *dl.cfg* is the configuration file that contains credentials and connections to **AWS resoruces (S3 , Redshift , IAM role)**.
##### Credentials in dl.cfg file have been omitted for security measures.
5) Other files are images : one of which is the Statsbomb company logo that's included in the project for citation reasons, 
and the other is a self-paced illustration diagram for the workflow of the  basic ETL pipeline.

### Future Work
* To use **AWS EC2** clusters to leverage the **Spark Standalone mode** capabilities.
* To use **Airflow** as a data orchestration tool for managing the big data pipeline smoothly using controlled **DAGs**.
* Complete the Star Schema Design of the **Redshift** Data warehouse .
* Extract insights / produce dashboards at the of the pipeline process .


# statsbomb-project
Applying data engineering process to the statsbomb open football data:

### **Capstone-Project-Template** file is the main program where it starts with:
* Applying the extaction / collection of data  
* Dumping the collected data directly into **AWS S3 buckets** instead of downloading it to disk .
* Transformation of the data using PySpark takes place by reading the data placed in the **AWS S3 buckets** .
* Then Copying/Loading the data into the **AWS Redshift** .

Other subsidary files completes the ETL pipeline as follwing :
1) *sql_queries_statsbomb* file conatins all the queries of our project , these queries drop , creates , inserts and copies the data.
2) *create_tbls_statsbomb* file executes the dropping and creating queries for our tables.
3) *etl_statsbomb-checkpoint* file executes the queries that do the copying and inserting of the data into the tables.
4) *dl.cfg* is the configuration file that contains credentials and connections to **AWS resoruces (S3 , Redshift , IAM role)**.
#### Credentials in dl.cfg file have been omitted for security measures .



class start_process():
    def __init__(self,file_name):
        '''
        initialte the spark session and load the file which is present in the directry
        file_name: path to the csv file type string.
        read the table with schema
        schema
        |-name -- string
        |-sku -- string
        |-description -- string
        '''
        import pyspark
        self.spark = (pyspark.sql.SparkSession.builder
                      .master("local")
                      .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                      .config("spark.dynamicAllocation.enabled","true")
                      .appName("Assignment").getOrCreate())
        self.schema = "name string,sku string, description string"
        self.df0 = (self.spark.read
                   .option("header",True)
                   .format("csv")
                   .option("multiline",True)
                   .option("delimiter", ",")
                   .schema(self.schema)
                   .load(file_name))
        del(pyspark)
        print('data read complete')
    def update(self, name=None, sku=None, description=None, updatedata_path=None,insert=False):
        '''
        upsert the table either with the path of update dataframe or with the row values of name, sku, description.
        default it will take path if not provided we need to give row values of name, sku, description.
        need to pass the update table with schema
        schema
        |-name -- string
        |-sku -- string
        |-description -- string
        want to insert the new rows if not there in dataframe pass insert = True.
        '''
        (self.df0.write
         .format("delta")
         .mode("overwrite")
         .save("products"))
        print('data write complete')
        from delta.tables import DeltaTable
        df0 = DeltaTable.forPath(self.spark, "products")
        if updatedata_path:
            update = self.spark.read.format("delta").load(updatedata_path)
        elif (sku!=None and name!=None and description!=None):
            update = (self.spark.sparkContext.parallelize([(name, sku, description)])
                      .toDF(schema="name string,sku string, description string"))
        if insert:
            (df0.alias("df0")
                .merge(update.alias("update"), "df0.sku = update.sku")
                .whenMatchedUpdate(set = 
                                   {
                                       "name" : "update.name",
                                       "description" : "update.description"
                                   }
                                  )
                .whenNotMatchedInsert(values =
                    {
                      "sku": "update.sku",
                      "name" : "update.name",
                      "description":"update.description"
                    }
                                     )
               ).execute()
        else:
            (df0.alias("df0")
                .merge(update.alias("update"), "df0.sku = update.sku")
                .whenMatchedUpdate(set = 
                                   {
                                       "name" : "update.name",
                                       "description" : "update.description"
                                   }
                                  )
               )
        del(update)
        del(df0)
        del(DeltaTable)
        self.df0 = self.spark.read.format("delta").load('products')
        print("update complete")
    def save_table(self, db_name, table_name, db_location=None):
        '''
        save the table into the database provided with the location provided.
        schema of output table
        schema
        |- name -- string
        |- no_of_products -- bigint
        '''
        from pyspark.sql.functions import count
        self.df1 = (self.df0
                    .groupby("name")
                    .agg(count("*").alias('no_of_products')))
        self.table = table_name
        if db_location:
            self.db_location = db_location
            self.spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '{}'".format(db_name, db_location))
            self.spark.sql("USE {}".format(db_name))
            self.df1.write.mode("overwrite").saveAsTable(table_name)
            print("you table saved in",db_name,"database with table name",table_name, "in location",db_location)
        else:
            self.db_location = None
            self.db_name = db_name
            self.spark.sql("CREATE DATABASE IF NOT EXISTS {} ".format(db_name))
            self.spark.sql("USE {}".format(db_name))
            self.df1.write.mode("overwrite").saveAsTable(table_name)
            print("you table saved in",db_name,"database with table name",table_name)
        del(count)
        self.db = db_name
    def clean(self,value=all):
        '''
        pass value='table' to delete table data.
        pass value='db' to delete db data
        pass value=all to remove all
        default will be all
        '''
        import shutil
        if value==all:
            del(self.df0)
            del(self.df1)
            del(self.spark)
            if self.db_location:
                shutil.rmtree('spark-warehouse/{}'.format(self.db_location))
            else:
                shutil.rmtree('spark-warehouse/{}.db'.format(self.db_name))
            self.spark.stop()
        elif value=='db':
            if self.db_location:
                shutil.rmtree('spark-warehouse/{}'.format(self.db_location))
            else:
                shutil.rmtree('spark-warehouse/{}.db'.format(self.db_name))
        elif value=='table':
            if self.db_location:
                shutil.rmtree('spark-warehouse/{}/{}'.format(self.db_location, self.table))
            else:
                shutil.rmtree('spark-warehouse/{}.db/{}'.format(self.db_name, self.table))
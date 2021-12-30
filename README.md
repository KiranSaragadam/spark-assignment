# spark-assignment
assignment
requirements for this file:

  1.pyspark==3.1.2
  
  2.shutil

import the spark_file.py file into ur notebook

<pre>import spark_file</pre>

at first we need to initiate the spark session by using the following code we need to pass csv file location when starting the session.

<pre>table = spark_file.start_process("products.csv")</pre>

the data was read by the spark session and saved with the attribute df0.

to update the row in the dataframe we can use update function by passing either path to the update dataframe 
or we can pass row values which contains name, sku,description as args in update function.

way to update table using path to update table which is in delta format.
<pre>table.update(updatedata_path = path) </pre>

way to update using row values:

<pre>table.update(name = "hello", sku = 'lay-raise-best-end', description = 'new description') </pre>

if u want to insert new row if the update rows are not matched then pass insert = True

<pre>table.update(name = "hello", sku = 'new-row', description = 'new description',insert = True)</pre>

if not they wont saved in the saved

the updated table is present in the products directry in delta format.

to save the table as database there is save_table function.

to use save_table function we need to pass db_name, table_name to create database with the aggregate table.

<pre>table.save_table('db_name', 'table_name')</pre>

we can pass path with db_location argument to save the database data in the path provided.

<pre>table.save_table('db_name', 'table_name', 'db_location')</pre>

if the database is in create state and table with same name present in the database then this save_table throws error table data is already present in the database in the location instead of dropping the database or table data i created clean function to clean the db data/table data using shutil library of python

clean function takes 3 values all/'db'/'table'

if we pass 'db' it will remove the database

if we pass 'table' it will remove the table in the database.

if we pass all it will remove the variables created till now with database.

dafault value will be all

<pre>table.clean(all)</pre>

when reading the csv file following schema will applied:

<pre>root
 |-- name: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- description: string (nullable = true)</pre>

when updating the data we need to farword data with following schema:

<pre>root
 |-- name: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- description: string (nullable = true)</pre>

when saving the data into the database we will get aggregate table with following schema:

<pre>root
 |-- name: string (nullable = true)
 |-- no_of_products: long (nullable = false)</pre>

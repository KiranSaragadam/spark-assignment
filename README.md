<h1><center>spark-assignment</center></h1>
<p>
    The code in this file helpfull in reading the provided csv as spark dataframe and 
    creating the <strong>database</strong> with the <strong>aggregated table</strong> 
    with desired names 
</p>
<p>
    This code has 3 functions
    <ol>
        <li>update</li>
        <li>write_table</li>
        <li>clean</li>
    </ol>
</p>
<p>
    Below steps need to follow to use this code.
</p>
<ol><li><strong>Initialisation</strong></li>
<ul>
<li>
import the spark_file.py file into ur notebook</li>

<pre>import spark_file</pre>

<li>at first we need to initiate the spark session by using the following code
     we need to pass csv file location when starting the session.

<pre>table = spark_file.start_process("products.csv")</pre>

the data will read by the spark engine and saved with the object df0.</li></ul>

<li><strong>Update</strong></li>
  
<ul> 
<li>To update the rows in the dataframe we can use update function by passing either path to the update dataframe 
or we can pass row values which contains name, sku,description as args in update function to update single row.</li>

<li>to update follow any one of the below ways </li>
  
  
   1) way to update table using path to update table which is in delta format.
     <pre>table.update(updatedata_path = path) </pre>

   2) way to update using row values:
     <pre>table.update(name = "hello", sku = 'lay-raise-best-end', description = 'new description') </pre>

   3) if u want to insert new row if the update rows are not matched then pass insert = True with row values:
    <pre>table.update(name = "hello", sku = 'new-row', description = 'new description',insert = True)</pre>

      - if not they wont inserted into table if the match not found.
      - the updated table is present in the products directry in delta format.
</ul>

  
<li><strong>Save Table</strong></li>
  
  - the main objective of this assignment is to save the aggregrate table in the database.
  - there is save_table function which requre db,table name to create table within database.
  - if we want to save the database with desired location then we can pass db_location argument if needed.
<ul>
  <li>to save database follow any one of the below ways</li>
  
  1) save database with table using default location:
     <pre>table.save_table('db_name', 'table_name')</pre>
  
  2) save database with table with custom location we provide:
     <pre>table.save_table('db_name', 'table_name', 'db_location')</pre>
  </ul>
  - if the database is in create state and table with same name present in the database then function throws error <code>Can not create the managed table(table_name). The associated location(db_locaton+table_name) already exists</code> instead of dropping the database or table data I created clean function to clean the db data/table data using shutil library of python

  <li>Clean</li>
  
    
- clean function takes 3 values all/'db'/'table'
  1) if we pass 'db' it will remove the database.
  <pre>table.clean('db')</pre>
  2) if we pass 'table' it will remove the table in the database.
  <pre>table.clean('table')</pre>
  3) if we pass all (default) it will remove the variables created till now with database.
  <pre>table.clean(all)</pre>
</ol>
<h3>Schema</h3>

- when reading the csv file following schema will applied:
<pre>root
 |-- name: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- description: string (nullable = true)</pre>

- when updating the data we need to farword data with following schema:
<pre>root
 |-- name: string (nullable = true)
 |-- sku: string (nullable = true)
 |-- description: string (nullable = true)</pre>

- when saving the data into the database we will get aggregate table with following schema:
<pre>root
 |-- name: string (nullable = true)
 |-- no_of_products: long (nullable = false)</pre>

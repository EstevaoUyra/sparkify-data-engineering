# The purpose of this database

This database has been created for a fictional startup called Sparkify. They want to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Database Schema and ETL Pipeline

![schema](rds.jpeg)
We decided to use a star schema, which 

State and justify your database schema design and ETL pipeline.


# How to run the project

First run `python create_tables.py`. That will drop existing tables and start them anew. 

Then run `python etl.py`. That will run the simple ETL to put data into our Postgres. If you want to take a look into what happens at each step on the ETL, you can 

Finally, open the notebook `test.ipynb` and run all cells to get a grasp on the data inside each table. You can also create your own queries to run.

## Example queries

[Optional] Provide example queries and results for song play analysis.
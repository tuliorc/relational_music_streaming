# Building a ETL Pipeline for Sparkify

This project summarises efforts to extract raw songplay data and log data, transform those and load them into a relational database consisted of 5 tables: 1 fact table (songplays) and 4 dimension tables (songs, artists, users, and datetime). 

# Files

### data
Folder containing all of the raw songplay and log data.

### sql_queries.py
Contains all of the necessary queries for creating tables and inserting data into them.

### create_tables.py
Python script that resets the database by droping any existing tables and then creating them again with no rows.

### test.ipynb
Jupyter Notebook aimed at testing tables creation and data insertion.

### etl.py
Main Python script for extracting raw data, transforming certain columns and loading them into the relational database.

### etl.ipynb 
Jupyter Notebook with the purpose of being a draft file before summing up code into etl.py.


# How to run

Clone the remote repository:
```
git clone https://github.com/tuliorc/relational_music_streaming.git
```

Go into your new local repository:
```
cd relational_music_streaming
```

Make sure you have Python3 installed in your computer:
```
python -V
```

In case you don't, install it:
```
sudo apt-get update
sudo apt-get install python3.6
```
Execute the script for creating tables:
```
python3 create_tables.py
```
Then, execute the ETL script:
```
python3 etl.py
```
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

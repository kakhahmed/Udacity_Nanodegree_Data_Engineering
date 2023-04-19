# Football player appearance data analysis
### Data Engineering Capstone Project

#### Project Summary
Create a data warehouse analyzing football player appearances based on multiple source from `football_data_transfer_market` for European competitions. Either raw CSV datasets or a normalized JSON dataset.
    - Player appearance analysis: By analyzing various player stats such as goals, assists, yellow_cards, red_cards. One can evaluate the performance of individual players and identify areas where they need to improve.

#### Contents:

- Capstone_Project.ipynb: The main ETL notebook which has all needed steps for completing the project.
- requirements.txt: The file with all needed dependencies to run the ETL
- etl.py: The python module to run the pipeline using spark-script for example.
- prepare_raw_data.ipynb: This notebook is used to create one of the main two datasets of this project. It produces JSON combined data source.
- tests/test_etl.py: consists of unit tests using pytest for the functions used in our pipeline.
- raw_data: Unlike the other CSV data sources used. This one is included as it was made specially for this project.

# Datasets used

`Football Data from Transfermarkt`: https://www.kaggle.com/datasets/davidcariboo/player-scores
    - Football (Soccer) data scraped from Transfermarkt website
    The dataset is composed of multiple CSV files with information on competitions, games, clubs, players and appearances that is automatically updated once a week. Each file contains the attributes of the entity and the IDs that can be used to join them together.
`player_appearance`: JSON made data source from Football Data from Transfermarkt by combining two datasources together using `prepare_raw_data.ipynb`

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

# Step 1: Scope the Project and Gather Data

#### Scope

Create analytics database for football player appearances in European competitions:

- Use spark to load the raw datasets.
- Explore the datasets for `player_appearances` and perform data cleaning.
- Read and create dimension tables from `games`, `clubs`, `competitions`, `player_appearances` datasets.
- Write fact and dimension tables to parquet files.
- Apply data quality checks on all table and raise error if a check failed.


### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

![Conceptual Data Model](data_model.png)

The fact table is made up from the JSON raw dataset `players_appearances`. This fact table consists of the appearance attributes for players in every minute of the game.

The needed dimensions for this model are:
- players: for details related to the player origin, position, and age.
- games: for details related to the place of the game, managers, clubs, and goals.
- competitions: to track the main competition for every game a table with the competitions details is needed.
- countries: This table has only the countries with competitions included in our data model.
- time: for a more detailed time attributes, this table splits the appearance dates into more dimensions of interest.

All the dimensions tables are collected from CSV datasets from football_data_transfer_market original data. Except for players and time tables, which are similar to player_appearance table are collected from the JSON raw data files.

All dimension table link to the fact `player_appearance` table through the date and different primary IDs in every dimension.

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

- Load the raw datasets.
- Investigate the players_appearances raw dataset.
- Applying some cleaning for the players_appearances raw dataset.
- Create dimensions tables: [players, games, clubs, competitions, countries, time]
- Create fact table: player_appearance.
- Apply data quality checks over dimension and fact table.


### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
- Build the data pipelines to create the data model.

    - The pipeline consists of:
      - etl.py: the main pipeline module.
      - tests/test_etl.py: unit tests for the pipline functions.
      - requirements.txt: The file containing all dependencies.
      - `pip install -r requirements.txt` should be sufficient to install all dependencies.
    - The pipeline can be triggered by running etl.py. It should be possible to run it as a spark script using `spark-submit etl.py `
#### 4.2 Data Quality Checks
 
Run Quality Checks:  
1- Data availability by checking that all data frames exists.  
2- Table data completeness and no missing rows for any table. 

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

    - Refer to Capstone_Project.ipynb

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.

* Choice of tools and technologies for the project:
  - Dataset: The `football_data_transfer_market` dataset was chosen for its freshness, size, and source credibility.
  - Spark: It was used for speed by performing in-memory computations, scalability when the data size increase we can increase number of used clusters, flexibility in python and task versatility with large datasets.

* Propose how often the data should be updated and why.
  - Main source of data is at [Kaggle](https://www.kaggle.com/datasets/davidcariboo/player-scores) is updated weekly. Hence, weekly update is proposed.

* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
    - As we are already using spark, we are able to scale our pipeline by increasing the number of partitions and size of Spark cluster, optimize the data pipeline by tuning Spark configuration parameters, use distributed file system like Hadoop or Amazon S3 to distribute data across multiple nodes.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
    - Optimize data extraction.
    - Use efficient data storage, The used parquet should already help in this as it can provide fast access to the data.
    - Airflow can be very helpful in orchestrating and scheduling the data pipeline.
    - Monitoring and alerting to identify issues and errors before they become critical. Tools like Nagios can help.
 * The database needed to be accessed by 100+ people.
    - Make sure database can handle increased number of users and their queries by scaling up hardware resources.
    - Consider using distributed architecture for example using distributed file system like Hadoop to handle large datasets and provide fault tolerance.
    - Redshift can be good solution as it handle large amounts of data and provide fast query performance.
    - Monitor and optimize the pipeline regularly using Apache Spark monitoring
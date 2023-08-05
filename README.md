## Intro

## Table of contents

⚠️ This repo only contains the files I've changed since some of the files in the original repo were too big in size for GitHub.

## Introduction

The challenge was:

> Based on the aforementioned data sets, you should be able to calculate which top 3 industries fluctuated the most (whether positive or negative) in the last 24 hours of orders comparing to the historical data of the last 30 days.


At first I had thought the assignment required a more complex use case and my original idea was to try and stream data from Kafka and from that (or from streaming the **orders** table) create a window of 30d and another of 24h to get the needed comparisons. Eventully it became clear that was not the case and what was required was to have the data stored and some process one could run to aggregate the data related to the last 24h. as if it was scheduled on an orchestrating solution or a crontab.

Since this is the case, I'm streaming data from Kafka for the orders and splitting it into two tables, one for the *orders* and another for the *order_lines* (although these aren't needed for the assignment), while also keping a file strem on a path in Minio to receive new Customers entries.

There's an initial job to load the data previously generated and another one to aggregate it to create an answer to the problem statement and store it in a new table.

## Folder structure
I've tried to keep a structure similar to the one in the original repo. I've added an **infra** folder for the files tha where changed regarding infrastructure. **notebooks**, **spark-appsn** are self-explanatory and need only be copied into the original repo.

```
├── README.md
├── infra
│   └── copy_to_minio.sh
├── notebooks
├── spark-apps
│   ├── initial_loader.py
│   ├── orders.py
│   ├── schemas
│   └── utils.py
└── spark-apps-python
```

## Changes made to infra:
- In **docker-compose.yml** I've changed the version of the mariadb image from *latest* to  *10.9.7*, as a fix for an error such as described [here](https://github.com/bitsondatadev/trino-getting-started/issues/31#issuecomment-1611865456).
- In order to create a location for checkpointing in structured streaming, in file **copy_to_minio.sh** I've added the following line: `mc mb minio/checkpoints --ignore-existing` . Besides that, I've also created separate paths for each type of data file (Orders, Industries, Customers, Products)


## Data Model

In order to solve the assignment, the only info we need is how much a customer spent in each order, there's no need for details regarding what the order contains nor the products ordered, as well as what industry corresponds to each costumer.

For the final table, where the aggregated info would be kept, I propose one where we can keep the industry, the ranking (from 1 to 3 in terms of fluctiation, where 1 is the one that had a bigger change in the last 24h compared to the previous 30d), the day it refers to and also the 30d avg, 24h value, and the delta.

In a real case scenario where we would store all of the data shared in the assignment I would propose the following:

```mermaid
erDiagram
  Products {
    string product_id PK
    string product_name
    float price
  }

  Orders ||--|{ Order_Lines : contains
  Orders ||--|| Customers : has
  Orders {
    string order_id PK
    string customer_id FK
    float amount
    datetime timestamp
  }

  Order_Lines ||--|| Products : contains
  Order_Lines {
    string order_id PK,FK
    string product_id PK,FK
    int quantity 
    float price
  }

  Customers ||-|{O Industries : is part of 
  Customers {
    string customer_id PK
    string company_name
	  string industry FK
  }

  Industries {
    string industry PK
    ingest_date date
  }

  Fluctuations {
	string industry PK
	date agg_date PK
	float string[30d_avg]
	float string[24h_value]
	float delta
	int rank
  } 
```


## How does it work

There are multiple jobs used in this solution. One for each of the tasks needed: load the initial data, stream data from Kafka, consume batch data, and aggregate data. I will go through each in this section.
In the **schemas** folder, I'm storing both table DDL in a YAML file, as well as .py files with schemas to be used in Python Dataframes. I'm not a huge fan of how this is done at the moment, but there's no time for more.

### Initial Data Loader

In this job, all of the required tables are created and populated with the demo data.

### Customers batch flow
For the customer data, assuming this would be a CSV that gets uploaded from time to time, the ideal solution would be to have a service like AWS Lambda which would be triggered by new files in a certain bucket and process them.
In this case, the approach I've followed was to have a spark stream reading from the bucket, so that when a new file with data came into a specific folder, it would get processed. Then, I would save the data. For this, I've also created a new folder inside the **demo-data** bucket, as described in the infra section.

### Orders Stream Flow

**orders.py** has the code to continuously consume the orders data from Kafka. It will consume the data until the job is cancelled and will store it in three different places, making use of the *forEachBatch* function. It will sotre the raw data that comes from Kafka in a folder inside the **raw-data** bucket (in case there's a need to recover data, since Kafka isn't persistent) and it will populate both the **orders** table and the **order_lines** table at the same time, appending the new data to the table.

To run:

```./spark-jobs-python/run-in-spark.sh orders.py```

### Notebook

Lastly, there's a notebook that does the same as the aggregating job, but in SQL.

## Usage

XX is used to consume the orders from Kafka and store them in their respective tables (`orders`, and `order_lines`), as well as keeping a raw_data history in binary format - as in a bronze-silver structure.

YY is used to update the Customer data. It keeps track of the files inserted in the data-demo/Customers/ path in Minio and processes each new file to update the data in the `customers` table.

ZZ is the aggregating job. It uses a stream on the orders table to keep track of both the 30 day average, as well as the 24h values, compare them, and store them in the `fluctuations` table.

These are started with 
```
spark sbmutigfvd osjfshighids sjfigsdf
```
In the *schema* folder I'm storing the schemas used for each dataframe as well as yaml files which provide the configuration for each of the tables.

Finally, there's a notebook in the **notebooks** folder which has an SQL snippet to generate the same data as the one in the aggregation job in case someone wants to do ad-hoc querying of the same style (e.g. comparing with the last 7 days instead of 30)

## Comments

Unfortunately I did not have the time to do the assignment as I would've liked to, but it was a fun challenge that made me use some technologies I hadn't used in some time. I think

If I had more time I would spend it developing better solutions to make the apps more re-usable in the sense that creating a new Kafka stream would have it's boilerplate files and a way to configure whatever aditional parsing needed without having to write it in the consumer code as is right now. I would also create some type of class objects for the consumer for better organization.

Another two things missing and that I think should be included are tests and a commit pipeline which could run some tests those same tests and run stuff like flake or black to better the code. This way we could validate, besides the code, that the schemas were being correctly defined, among others.

Lastly, this solution doesn't acommodate schema evolution, that's another think I would try to implement with more time and thought.

Overall this was a fun challenge to make, but I found a lot of issues with infra. For quite some time I was having trouble connecting to the hive-metastore and since I was very time constrained (due to work and to the fact I requested the challenge on Monday , instead of further down the week to include the weekend) I'm not even sure how I ended up solving it, except trying to prune evertything docker related (containers, volumes, network). There some other issues, such as some trouble reading data from Kafka due to:

``````
Py4JJavaError: An error occurred while calling o235.start.
: org.apache.hadoop.ipc.RpcException: RPC response exceeds maximum data length
	at org.apache.hadoop.ipc.Client$IpcStreams.readResponse(Client.java:1936)
	at org.apache.hadoop.ipc.Client$Connection.receiveRpcResponse(Client.java:1238)
	at org.apache.hadoop.ipc.Client$Connection.run(Client.java:1134)
``````

Which I tried solving by adding the following line in **spark-defaults.conf**:
`spark.driver.maxResultSize              2g` - but again I'm not quite sure this was what solved it as I was trying different solutions at the sam time, the lack of time played a part in this as it should've been a more focused and better documented approach.

For reference, these are the specs I'm using, in a 2021 MacBook Pro:
- macOS: Ventura 13.4
- Chip: Apple M1 Pro
- Memory: 16GB

And I'm using version 4.21.1 of Docket Desktop.

Also, the example data you shared was from 2022, which made me think why my queries weren't working when I first tried aggregating the last 30d data. I've also changed the data generator script to generate data starting in 2023 and used a new dataset.

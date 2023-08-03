## Intro

## Table of contents



⚠️ This repo only contains the files I've changed since some of the files in the original repo were too big in size for GitHub.

## Reasoning


## Folder structure
I've tried to keep a structure similar to the one in the original repo. I've added an **infra** folder for the files tha where changed regarding infrastructure. **notebooks**, **spark-jobs-python**, and **spark-jobs-python** are self-explanatory and need only be copied into the original repo.

```
├── README.md
├── infra
├── notebooks
├── spark-jobs-python
└── spark-jobs-scala
```

## Changes made to infra:
- In **docker-compose.yml** I've changed the version of the mariadb image from *latest* to  *10.9.7*, as a fix for an error such as described [here]().
- In order to create a location for checkpointing in structured streaming, in file **copy_to_minio.sh** I've added the following line:

`mc mb minio/checkpoints --ignore-existing`

## How does it work


## Usage

## Further considerations

If I had more time this is what I would do next:




Overall this was a fun challenge to make, but I found a lot of issues with infra. For quite some time I was having trouble connecting to the hive-metastore and since I was very time constrained (due to work and to the fact I requested the challenge on Monday , instead of further down the week to include the weekend) I'm not even sure how I ended up solving it. There some other issues, such as some trouble reading data from Kafka due to:

``````
Py4JJavaError: An error occurred while calling o235.start.
: org.apache.hadoop.ipc.RpcException: RPC response exceeds maximum data length
	at org.apache.hadoop.ipc.Client$IpcStreams.readResponse(Client.java:1936)
	at org.apache.hadoop.ipc.Client$Connection.receiveRpcResponse(Client.java:1238)
	at org.apache.hadoop.ipc.Client$Connection.run(Client.java:1134)
``````

Which I solved by BLA BLA FIX ME

For reference, these are the specs I'm using, in a 2021 MacBook Pro:
- macOS: Ventura 13.4
- Chip: Apple M1 Pro
- Memory: 16GB

And I'm using version 4.21.1 of Docket Desktop.

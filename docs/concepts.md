---
layout: page
title: Concepts
permalink: /concepts/
---
What are some of the the things you need to know when building data lakes? On this page, we'll define what we mean by a data lake, why you would like to build one, and how you can do that. 
* Do not remove this line (it will not be displayed)
{:toc}

## What is a data lake?
A data lake is a place where you bring all data of the organisation together. A data lake is implemented on a distributed file system such as HDFS, or in the cloud (AWS S3, Azure Blobstorage, Azure Data Lake Store, Google Cloud Storage, ...). Often data is stored in its original format (csv, json, avro, images, ...) and it is being processed by a big data processing engine, such as MapReduce, Spark, Flink or Hive. 

## Why do you need a data lake?
The big data hype train is running at full speed, and one of its wagons is the mystical data lake. The truth is that there is a good chance that your organisation needs neither. We can't assess for you whether your organisation needs a data lake, but we can share the reasons why other organisations chose to invest in it.

### Capture data first, ask questions later
One of the main benefits of a data lake compared to a data warehouse, is that you don't *need* to structure all the data that you capture. You can store all data in its original format. This allows for much greater variety of data than a typical data warehouse can handle. Also, data lakes are built to scale. So ingesting gigabytes, terabytes or even petabytes of data is no problem. In fact, data lakes often suffer from small data. Many small files can make both storage and processing slow and inefficient. So if your organisation has about 100MB of data to process, probably a data lake is not the right choice. However, if you want to combine clickstream data with your sales transactions, and you're collecting sensor data and images as well, maybe combined with millions and millions of rows of hardware logs, a data lake is perfect. 

### Complex analytics, dashboarding and data exploration
You probably want to do something with all the data you are capturing in the data lake. What exactly you want to do, depends on your organisation, your industry and your priorities. But a few patterns are emerging:
1. **Complex analytics**: Churn prediction, next product to buy, stock optimisation, workforce prediction, preventative maintenance, asset usage optimisation, ... These are the projects that can make a difference in your organisation, and this is, from a C-level perspective, probably the main reason why they invest in data lakes in the first place. 
2. **Dashboarding**: A little bit less sexy than complex analytics, but still extremely valuable to business. Dashboarding is not new, and are in fact one of the main outputs of a data warehouse. Sometimes dashboarding is also needed on large datasets that never end up in the data warehouse. In that case, it might make sense to calculate the aggregations on the data lake and build a few dasbhoards on top. 
3. **Data exploration**: As simple as it sounds, it's often not a trivial task to understand which data sources are available in an organisation and what they look like. They are often (rightfully so) locked up in operational systems, and only a small percentage ends up in the data warehouse. This results in the vast majority of people having almost no access to the company data. The cost associated with this organisational blind spot is often under-estimated. As organiations become more data driven and fact based, it is critical that those facts end up seemlessly in the hands of as many people as possible.    

### Reuse data across use cases
  
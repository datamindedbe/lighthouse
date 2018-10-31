---
layout: page
title: Concepts
permalink: /concepts/
---
What are some of the the things you need to know when building data lakes? On this page, we'll define what we mean by a data lake, why you would like to build one, and how you can do that. 
* Do not remove this line (it will not be displayed)
{:toc}

## What is a data lake?
A data lake is a method of storing data within a system or repository, in its natural format, that facilitates the collocation of data in various schemata and structural forms, usually object blobs or files. The idea of data lake is to have a single store of all data in the enterprise ranging from raw data (which implies exact copy of source system data) to transformed data which is used for various tasks including reporting, visualization, analytics and machine learning. The data lake includes structured data from relational databases (rows and columns), semi-structured data (CSV, logs, XML, JSON), unstructured data (emails, documents, PDFs) and even binary data (images, audio, video) thus creating a centralized data store accommodating all forms of data. Source: [Wikipedia](https://en.wikipedia.org/wiki/Data_lake)

## Why do you need a data lake?
The big data hype train is running at full speed, and one of its wagons is the mystical data lake. The truth is that there is a good chance that your organisation needs neither. We can't assess for you whether your organisation needs a data lake, but we can share the reasons why other organisations chose to invest in it.

### Capture data first, ask questions later
One of the main benefits of a data lake compared to a data warehouse, is that you don't *need* to structure all the data that you capture. You can store all data in its original format. This allows for much greater variety of data than a typical data warehouse can handle. Also, data lakes are built to scale. So ingesting gigabytes, terabytes or even petabytes of data is no problem. In fact, data lakes often suffer from small data. Many small files can make both storage and processing slow and inefficient. So if your organisation has about 100MB of data to process, probably a data lake is not the right choice. However, if you want to combine clickstream data with your sales transactions, and you're collecting sensor data and images as well, maybe combined with millions and millions of rows of hardware logs, a data lake is perfect. 

### Reuse data across use cases
Another one of the main benefits of a data lake, is that you can reuse data to implement different use cases. For instance, your clickstream data might be useful for the web development team to improve the website layout. It might also be useful for the marketing department to combine it with retail transactions to see who first went to the site, and then bought the item later in the store? Another example is the famous "Customer360". It's a wide table, one row per customer, and every column describing something you know about the customer. For instance the number of products she has purchased in the last 6 months, the total amount spent in a category, whether the customer lives in an urban area, ... That Customer360 can be reused to do churn prediction, cross-sell, upsell, customer satisfaction improvement, ...  If you're only interested in one data source, eg the clickstream data, putting it in a data lake doesn't add a lot of value. There are tools like Adobe Analytics which are perfect for understanding clickstream data. However, if you want to mix and match data, and if you want to reuse data sources to do many different things, a data lake is right for you.  

### Data analytics
You probably want to do something with all the data you are capturing in the data lake. What exactly you want to do, depends on your organisation, your industry and your priorities. But a few patterns are emerging:
1. **Complex analytics**: Churn prediction, next product to buy, stock optimisation, workforce prediction, preventative maintenance, asset usage optimisation, ... These are the projects that can make a difference in your organisation, and this is, from a C-level perspective, probably the main reason why they invest in data lakes in the first place. 
2. **Dashboarding**: A little bit less sexy than complex analytics, but still extremely valuable to business. Dashboarding is not new, and are in fact one of the main outputs of a data warehouse. Sometimes dashboarding is also needed on large datasets that never end up in the data warehouse. In that case, it might make sense to calculate the aggregations on the data lake and build a few dasbhoards on top. 
3. **Data exploration**: As simple as it sounds, it's often not a trivial task to understand which data sources are available in an organisation and what they look like. They are often (rightfully so) locked up in operational systems, and only a small percentage ends up in the data warehouse. This results in the vast majority of people having almost no access to the company data. The cost associated with this organisational blind spot is often under-estimated. As organiations aim to become more data minded, it is critical that those facts end up seemlessly into the hands of as many people as possible.    

## How to build a data lake
By now, you know what a data lake is, and you probably have a good understanding of why you need one in your organisation. Now is the time to actually build a data lake. It's best to start with a particular use case. You may have a few lined up already. In that case, pick one, and build it end-to-end. That use case should generate business value. And hopefully, you can claim back some of that business value to invest in more use cases. Compare it to a freight train: it takes a while to build momentum, but once it's on its way, it can move a lot of work. Your first use case will take the longest. Your second use case should already be somewhat easier because now you actually know what you're doing (honestly, it's true). And also, you might be able to reuse some of the data sources that you built for the first use case.

Let's look at a data lake component per component:

### Storage
First and foremost, a data lake is supposed to store data. If you don't think this through up-front, chances are high your data lake turns into a data swamp very quickly. Different teams store data in all kinds of formats, on the weirdest locations. In the end, your data lake bulks with data, but nobody really knows anymore where that data came from, who is responsible for it, or why it's even there in the first place. On the other hand, you don't want to enforce too strict data requirements either, or you risk to miss out on one of the main values of the data lake: capture the data first, ask questions later. 

#### Data formats
Unstructured data is a myth. While you want to be open to ingest any kind of data, it's a good idea to transform it to something common across the data lake as the first processing step. Columnar formats such as Parquet or ORC work really well because they are strongly typed, they allows for very efficient compression, and are very fast to use in analytics as well. The only downsides to columnar formats are that they are usually append-only and that they don't work really well with many small files. So if data comes in in a real-time streaming fashion, you usually have to compact all the small files of the last hour, into one big file.

Why do we want to do this? There are a couple of reasons:
1. **Don't repeat yourself**: You don't want to to apply the cleaning logic over and over again. This is a recurring issue. You see cleaning code copy&pasted everywhere, and obviously, bugs are only fixed in some of those copies. It's ugly and it clutters your data pipeline.
2. **Data exploration**: Discussed further below, but since all data is in the same, preferably structured, format, it is easy to expose it to data analists and data scientists.
3. **Detect issues early**: One of the most common reasons why a data lake job fails, is because there is something wrong with the source data. Either it's gone missing, its structure has changed, or the content looks completely different. A cleaning step can detect and surface those problems early.   

#### Topology of a data lake
One of the best ways to bring structure to your data lake, is by explicitly designing a *topology*, which boils down to putting a structure on your data. In the database world, you have the structure `database -> schema -> table`. And when you load new data, you have to think a bit about what the appropriate `table`, `schema` and `database` is for that data.   

The same rule applies to data lakes, and very often the structure is applied by putting files into folders. Which structure you need exactly depends again on the needs of your organisation. You can think of the folder structure as your `namespace` and the table name as your `key`. At the highest level, the root of your data lake should be divided into zones, or top-level namespaces. Here are some zones that we see coming back often:
1. `raw`: Sometimes also called `landing`, it's the place in your data lake where you store all the raw incoming data. You don't apply any transformations, you don't do any cleaning. You take the data as-is. The only process that ideally accesses the `raw` data, is the cleaning process, see below. 
2. `clean`: Often the first step in a data pipeline, is to clean data. You take in the raw files in CSV, JSON or any other format and you apply a schema to it. You remove the lines which don't comply to that schema. Ideally, you log the rejected lines in a `rejected` folder. Once data is clean, and in a uniform format, it is ready for consumption. Note that you typically don't want to apply business logic yet in the cleaning step. You just share what you observe in the respective source datasets. 
3. `master`: Some organisations have clearly defined Master data *somewhere*. For example, they have a single repository for Customers, a single repository for Products, a single repository for Assets, ... In that case, you could just copy that data straight to `master`. But more often than not, reality is more complex, and not a single source system can provide this master data. So this is your chance to build it. Again, tradeoffs are needed. You probably don't want to end up with 13 different definitions of a *Customer*. But aligning an entire organisation on what you mean when you say *Customer*, often takes months of meetings, and is (hopefully) out-of-scope for the data lake project. Building a `master` data asset usually means joining a set of `clean` tables, and applying business logic. 
4. `metrics`: Metrics that are calculated once, and then are reused across projects, are often stored in a `metrics` zone. A Customer360 is a good example of a set of metrics that are calculated about a particular entity, in this case customer, that is reused across projects.  
5. `projects`: Each use case you implement in the data lake also has their own datasets. Maybe you build some tables that are used for intermediate calculation, or simply a set of metrics that is probably not relevant for other projects. 
6. `export`: Sometimes you can choose to put the tables you export for further use downstream in a separate `export` zone. For instance, your customer churn prediction model can put data in `export` daily about which customer it thinks will churn in the next 3 months. You could also store that in the project-specific zone. Systematically storing these tables in `export` makes it clear which datasets are consumed by the outside world.  

Again, which zones you have in your data lake really depends on your particular situation. That flexibility is really convenient. But don't confuse it with *I can dump any data in any folder at any time*. It's a recipe for the disaster. 

Just like namespaces in code, namespaces in a data lake can be hierarchical. Within a zone, you can have further subdivisions.  Typically `raw` and `clean` reflect where the data came from originally. Eg `/raw/internal/erp/warehouses/list.csv` -> `/clean/internal/erp/warehouses/file.orc`. In the other zones, you usually build assets in a structure that makes sense to the data lake.  

#### Partitioning
Having fresh data is important. The data of yesterday is orders of magnitude more relevant than the data of last month, which in turn is orders of magnitude more relevant than the data of last year. So, in most cases, you want to partition your data by year, month and day. It can be as simple as adding the partitioning to the path of your file, eg `/clean/internal/erp/warehouses/2018/02/24/file.orc`. There are some subtle nuances however:
1. **Full load**: Typical reference data sources are ingested completely. The example above illustrates this. An organisation has a list of warehouses, which is small data (nobody has millions of warehouses) and it is slowly changing (nobody opens multiple new warehouses per day). A typical approach is to ingest that list of warehouses daily in your data lake, so you know you always have the latest list. Even better, you can always go back in time and look what the list of warehouses was like 8 months ago. The storage cost for this kind of reference data is usually negligible.
2. **Incremental load**: Bigger data sources which come in at a higher velocity are loaded incrementally. Imagine you are processing clickstream data. You don't want to process all clicks over and over again. Instead, you apply an incremental loading and processing approach, where you only process the new clicks. That might end up here: `/clean/website/clickstream/2018/02/24/file.orc`.  So even though those paths look the same, one contains the complete state as it was on `2018/02/24` while the other only contains data from `2018/02/24`. Usually it's clear when incremental loading is being done. If you want, you can add `incremental` to every path with incremental loading. In that way, there is never any confusion.
3. **Partitioning in your data pipeline**: Note that if you ask a tool such as Spark or Hive to do the partitioning for you, the paths again look slightly different. 

   

#### Storage technologies
Where do you store all the data? This is a big decision and it depends on what's already available in the organisation. If you run your workloads on-premise, [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) is one of the most popular options. But actually, almost nobody installs HDFS standalone. The recommended approach is to install a Hadoop distribution, which includes HDFS, on a cluster of bare metal machines. Some popular Hadoop distributions are [Cloudera](https://www.cloudera.com/products/enterprise-data-hub.html), [Hortonworks](https://hortonworks.com/) or [MapR](https://mapr.com/). 

Most organisations are moving away from on-premise big data clusters, towards cloud technologies. Some popular options are [AWS S3](https://aws.amazon.com/s3/), [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) and [Google Cloud Storage](https://cloud.google.com/storage/). 


### Processing technologies
TODO: Complete this section 

### Data ingestion
TODO: Complete this section

### Workflow management
TODO: Complete this section

### Security
TODO: Complete this section

### Interface with the outside world
TODO: Complete this section

  
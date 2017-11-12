# Data Analysis for Crypto Currency
---
## Background
### Business requirement
A cryptocurrency (or crypto currency) is a digital asset designed to work as a medium of exchange using cryptography to secure the transactions and to control the creation of additional units of the currency.Cryptocurrencies are classified as a subset of digital currencies and are also classified as a subset of alternative currencies and virtual currencies. Cryptocurrency has drawn great attention
in recent years. Actually some people has made a fortune by purchasing Bitcoin and other crytocurrencies.

### DataSet
https://coinmarketcap.com/ is an excellent website to view the CryptoCurrency Market Capitalizations. 
There is a Kaggle dataset https://www.kaggle.com/jessevent/all-crypto-currencies. The dataset shows closing market rate for every crypto currency including.
420,000 rows of daily closing market data for 848 coins/tokens over 5 years

### Objective
Make a data product for Cryptocurrency and conduct some analysis for it.

### Technical Skills
* Data Transportation: Kafka
* Data Computation: Spark
* Data Storage: Cassandra
* Data Coordination: Zookeeper

## Tasks
### Choose the Topic and design the pipeline
* The pipe includes : Kafka Spark Cassandra Zookeeper
### Use Kafka to transport data
* The data is stored in csv
* Read the data to Kafka
* Simulate the real-time tranportation
### Use Spark to analyze data
* Use Pyspark
* Use RDD computation
* Use Spark Streaming to inject the computation results to the kafka
### show Cassandra to store data
* Use Cassandra to store data
* Only Store the results we are interested in


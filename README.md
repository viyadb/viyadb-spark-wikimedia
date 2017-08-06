viyadb-spark-wikimedia
=======================

Example of using ViyaDB Spark processing backend based on Wikimedia updates stream coming through IRC channel.

# Running the Example

## Prerequisites

 * [Consul](http://www.consul.io)
 
## Building

```bash
mvn package
```

## Running

First, create needed keys in Consul by running:

```bash
cd consul/ ; ./create-conf.sh
```

Then, run the Spark streaming application:

```bash
spark-submit --class com.github.viyadb.spark.sample.wikimedia.Job \
    target/viyadb-spark-wikimedia_2.11-0.0.1.jar \
    --table "wikimedia"
```


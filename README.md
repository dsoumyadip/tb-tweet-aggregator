# tb-tweet-aggregator

There are two type of job. Batch and stream.

* **BATCH:**
  This Spark job reads historical tweets in JSON format and do multiple types of aggregation and saves the aggregated
  data to Firestore.  
  **HOW TO RUN:**
    * **For
      Dataproc:** `gcloud dataproc jobs submit pyspark --cluster tb-cluster --region=us-central1 batch/aggregation-historical-tweets.py -- "gs://twitter-battle-2/historical-tweets.json"`
    * **For in house Spark cluster:**  
      `spark-submit --master SPARK_MASTER_IP:PORT batch/aggregation-historical-tweets.py -- "gs://twitter-battle-2/historical-tweets.json"`  
      **Type of aggregations:**
        * Historical total tweet count by each handle.
        * Historical tweet count by 1 hour window. (Total + tweet sentiment level)
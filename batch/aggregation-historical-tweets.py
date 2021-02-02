import logging
import math
import sys

from google.cloud import firestore
from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, array, lit, concat, first

logging.basicConfig(level=logging.INFO)


def flatten_df(nested_df):
    """This function takes a nested pyspark dataframe and return a flattened dataframe
    Args:
        nested_df: Pyspark nested dataframe

    Returns:
        Flattened dataframe
    """
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)


FILE_NAME = sys.argv[1]
# FILE_NAME = "gs://twitter-battle-2/historical-tweets.json"

logging.info("Creating Spark session...")
# Create Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("tweet-aggregations") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

logging.info("Spark session has been created.")

logging.info("Initializing firestore client")
db = firestore.Client()

logging.info(f"Reading historical tweets from {FILE_NAME}...")
# Read historical tweets from cloud storage
df1 = spark.read.json(FILE_NAME)

# df.cache()  # Do not cache in case of large data

logging.info(f"Total number of historical tweets {df1.count()}")  # Count number of tweets in historical data

# df.printSchema()

# Filter tweets which are marked as batch
df2 = df1.filter(df1.update_type == 'batch')

logging.info(f"Total number of tweets to be processed for aggregation {df2.count()}")

logging.info(f"Calcuting number of total tweets by each user...")
total_tweets_till_now = df2.groupBy(df2.username).count()
# total_tweets_till_now.show()

total_tweets_till_now_list = list(map(lambda row: row.asDict(), total_tweets_till_now.collect()))

batch = db.batch()

for user in total_tweets_till_now_list:
    doc_ref = db.collection(u'tb-tweets-aggregated').document(user.get('username'))
    batch.set(doc_ref, user)

batch.commit()

# In second part total tweet by each handles by 1 hr window
logging.info(f"Calculating total tweets by each user at hour level...")
num_tweets_each_hour = df2.groupBy("username", "author_id", window("created_at", "1 hour")) \
    .count() \
    .select("username", "author_id", "window", col("window.start").alias("window_start"),
            col("window.end").alias("window_end"), col("count").alias("tot_cnt"))

# num_tweets_each_hour.show()
# num_tweets_each_hour_list = list(map(lambda row: row.asDict(), num_tweets_each_hour.collect()))

# Now check sentiment wise tweet count
logging.info(f"Calculating total sentiment wise tweets by each user at hour level...")
# First flatten the dataframe
df_flattened = flatten_df(df2)

# first bucketed sentiment scores
bucketizer = Bucketizer(splits=[-1, -0.25, 0.25, 1], inputCol="sentiment_score", outputCol="sentiment_score_integer")

df_bucketized = bucketizer.setHandleInvalid("keep").transform(df_flattened)

sentiment_labels = ["negative", "neutral", "positive"]
label_array = array(*(lit(label) for label in sentiment_labels))

df_bucketized_with_sentiment_label = df_bucketized.withColumn(
    "sentiment_score_label", label_array.getItem(col("sentiment_score_integer").cast("integer"))
)

# df_bucketized_with_sentiment_label.toPandas()

num_tweets_each_hour_sentimental = df_bucketized_with_sentiment_label.groupBy("username", "author_id",
                                                                              window("created_at", "1 hour"),
                                                                              "sentiment_score_label") \
    .count() \
    .select(col("username").alias("username_b"), col("author_id").alias("author_id_b"), col("window").alias("window_b"),
            col("window.start").alias("window_start_b"), col("window.end").alias("window_end_b"),
            "sentiment_score_label", col("count").alias("tot_sentiment_count"))

# Pivoting the data into single row
num_tweets_each_hour_sentimental_pivot = num_tweets_each_hour_sentimental \
    .groupBy("author_id_b", "window_start_b", "window_end_b") \
    .pivot("sentiment_score_label") \
    .agg(first("tot_sentiment_count"))

num_tweets_each_hour_sentimental_pivot.toPandas()

df3 = num_tweets_each_hour
df4 = num_tweets_each_hour_sentimental_pivot
joined_df = df3 \
    .join(df4, ((df3.author_id == df4.author_id_b) & (df3.window_start == df4.window_start_b) & (df3.window_end == df4.window_end_b)))

joined_df_sub = joined_df \
    .select("username", "author_id", "window", "window_start", "window_end", "tot_cnt", "negative", "neutral",
            "positive")

joined_df_sub = joined_df_sub.na.fill(0)

# joined_df_sub.cache()
# joined_df_sub.count()
# joined_df_sub.show()

joined_df_sub = joined_df_sub \
    .select("*", concat(col("username"), lit("#"), col("window_start"), lit("#"), col("window_end")).alias("row_key"))

# joined_df_sub.show()

aggregation_list = list(map(lambda row: row.asDict(), joined_df_sub.collect()))
# aggregation_list


# Loop index
doc_no = 0
i = 1
max_loop = math.ceil(len(aggregation_list) / 500)

logging.info(f"Saving aggregated data into firestore...")
# We are doing this thing because firestore does not support more than 500 entries in a single write
while i <= max_loop:
    for item in aggregation_list[doc_no:doc_no + 500]:
        batch = db.batch()
        doc_ref = db.collection(u'tb-tweets-timeline').document(item.get('row_key'))
        batch.set(doc_ref, item)
        batch.commit()
        doc_no += 500
    i += 1

logging.info(f"Job has been completed")

from apache_beam.options.pipeline_options import PipelineOptions

from constants import *
from transformers import *


# def printer(element):
#     print(element)
#     return element


def main_pipeline(run_local):
    pipeline_options = {
        'project': PROJECT,
        'streaming': True,
        'staging_location': 'gs://' + BUCKET + '/dataflow/staging',
        'runner': 'DataflowRunner',
        'job_name': JOB_NAME,
        'disk_size_gb': 100,
        'temp_location': 'gs://' + BUCKET + '/dataflow/staging',
        'save_main_session': True
    }

    if run_local:
        pipeline_options['runner'] = 'DirectRunner'

    options = PipelineOptions.from_dictionary(pipeline_options)

    with beam.Pipeline(options=options) as pl:
        tweets = (pl | "read" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC_NAME)
                  | "extract" >> beam.ParDo(ByteStringToDict())
                  )

        windowed_tweets = (tweets
                           | "window" >> beam.WindowInto(beam.window.FixedWindows(WINDOW_DURATION_IN_SEC)))

        tweet_with_sentiment_label = (windowed_tweets
                                      | "bucket_sentiment" >> beam.ParDo(BucketSentimentScore()))

        grouped_tweet = (tweet_with_sentiment_label
                         | "group_by" >> beam.GroupBy(username=lambda u: u['username'],
                                                      author_id=lambda u: u['author_id'])
                         .aggregate_field(lambda a: a['sentiment']['sentiment_label'] == 'positive', sum, 'positive')
                         .aggregate_field(lambda a: a['sentiment']['sentiment_label'] == 'neutral', sum, 'neutral')
                         .aggregate_field(lambda a: a['sentiment']['sentiment_label'] == 'negative', sum, 'negative')
                         | "attach_windowed_timestamp" >> beam.ParDo(AttachTimeStamp())
                         | "convert_to_dict" >> beam.Map(convert_to_dict)
                         # | "print" >> beam.Map(printer)
                         | "write_to_firestore" >> beam.ParDo(FirestoreWrite(PROJECT, FIRESTORE_COLLECTION))
                         )


if __name__ == '__main__':
    run_locally = False
    main_pipeline(run_locally)

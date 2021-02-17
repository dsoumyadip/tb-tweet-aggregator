import ast

import apache_beam as beam
from google.cloud import firestore


class ByteStringToDict(beam.DoFn):

    def process(self, element):
        elem_str = element.decode("UTF-8")
        a = ast.literal_eval(elem_str)
        return [a]


class BucketSentimentScore(beam.DoFn):

    def process(self, element):
        if element['sentiment']['score'] < -0.25:
            element['sentiment']['sentiment_label'] = 'negative'
        elif element['sentiment']['score'] < -0.25:
            element['sentiment']['sentiment_label'] = 'positive'
        else:
            element['sentiment']['sentiment_label'] = 'neutral'
        return [element]


class AttachTimeStamp(beam.DoFn):

    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        element_new = [element + (window_start, window_end)]
        return element_new


def convert_to_dict(element):
    new_elem = dict()
    new_elem['row_key'] = (element[0] + "#" + str(element[5]) + "#" + str(element[6]))
    new_elem['username'] = element[0]
    new_elem['author_id'] = element[1]
    new_elem['positive'] = int(element[2])
    new_elem['neutral'] = int(element[3])
    new_elem['negative'] = int(element[4])
    new_elem['window_start'] = element[5]
    new_elem['window_end'] = element[6]
    return new_elem


class FirestoreWrite(beam.DoFn):

    def __init__(self, project, collection):
        self._project = project
        self._collection = collection
        super(FirestoreWrite, self).__init__()

    def process(self, element, *args, **kwargs):
        self._flush(element)

    def _flush(self, element):
        db = firestore.Client(project=self._project)
        db.collection(self._collection).document(element['row_key']).set(element)

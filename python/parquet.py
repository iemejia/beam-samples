import apache_beam as beam
import pyarrow as pa

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.parquetio import ReadAllFromParquet

class _PrintFn(beam.DoFn):
    def process(self, element):
        print(element)


if __name__ == "__main__":
    p = beam.Pipeline(options=PipelineOptions())

    RECORDS = [{'name': 'Thomas',
                     'favorite_number': 1,
                     'favorite_color': 'blue'},
                    {'name': 'Henry',
                     'favorite_number': 3,
                     'favorite_color': 'green'},
                    {'name': 'Toby',
                     'favorite_number': 7,
                     'favorite_color': 'brown'},
                    {'name': 'Gordon',
                     'favorite_number': 4,
                     'favorite_color': 'blue'},
                    {'name': 'Emily',
                     'favorite_number': -1,
                     'favorite_color': 'Red'},
                    {'name': 'Percy',
                     'favorite_number': 6,
                     'favorite_color': 'Green'}]

    SCHEMA = pa.schema([
        ('name', pa.string()),
        ('favorite_number', pa.int64()),
        ('favorite_color', pa.string())
    ])

    names = p | beam.Create(RECORDS)
    names | beam.io.WriteToParquet(file_path_prefix="/home/ismael/datasets/beam", schema=SCHEMA, codec='SNAPPY')

    # names = p | beam.io.ReadFromParquet(file_pattern="/home/ismael/datasets/parquet/gdelt-uncompressed.parquet/part-00000-10686784-4cdb-4497-b81d-2f64484900ca-c000.parquet")

    names = (
        p
        # | beam.Create(["/home/ismael/datasets/parquet/gdelt-uncompressed.parquet/part-00000-10686784-4cdb-4497-b81d-2f64484900ca-c000.parquet"])
        | beam.Create(["/home/ismael/datasets/parquet/gdelt-gzip.parquet/part-00000-dad2f869-1819-41d1-b768-bac35442676a-c000.gz.parquet"])        
        # | beam.io.ReadAllFromParquet()
        | beam.io.parquetio.ReadAllFromParquetBatched()
        | beam.Map(lambda batch: batch.to_pandas())
    )

    names | beam.ParDo(_PrintFn())

    p.run().wait_until_finish()

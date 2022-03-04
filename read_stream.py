import boto3
import json


def get_local_client(resource_name):
    return boto3.client(resource_name,
                        aws_access_key_id="dump",
                        aws_secret_access_key="dump",
                        endpoint_url='http://localhost:8000',
                        region_name='us-east-1')


class readStream:

    def __init__(self):
        self.local_dynamodb_client = get_local_client('dynamodb')
        self.local_dynamodb_streams_client = get_local_client('dynamodbstreams')
        self.records = []

    def get_stream_arn(self):
        describe_table_response = self.local_dynamodb_client.describe_table(TableName='Users')
        return describe_table_response['Table']['LatestStreamArn']

    def get_shards(self, stream_arn):
        describe_stream_response = self.local_dynamodb_streams_client.describe_stream(StreamArn=stream_arn, Limit=100)
        return describe_stream_response['StreamDescription']['Shards']

    def get_record_from_shard(self, shard_iterator):
        records_in_response = self.local_dynamodb_streams_client.get_records(ShardIterator=shard_iterator, Limit=1000)
        return records_in_response['Records']

    def run(self):
        records_dict = {}
        while True:
            stream_arn = self.get_stream_arn()
            shards = self.get_shards(stream_arn)

            for shard in shards:
                iterator = self.local_dynamodb_streams_client.get_shard_iterator(
                    StreamArn=stream_arn,
                    ShardId=shard['ShardId'],
                    ShardIteratorType='TRIM_HORIZON',
                )
                shard_iterator = iterator['ShardIterator']

                while shard_iterator:
                    res = self.local_dynamodb_streams_client.get_records(ShardIterator=shard_iterator)
                    self.records = res['Records']
                    shard_iterator = res['NextShardIterator']

                    if shard['ShardId'] in records_dict:

                        len_current_records = int(len(records_dict[shard['ShardId']]))
                        # print(len_current_records)
                        # print(int(len(self.records)))

                        if len(self.records) > len_current_records:
                            new_records = self.records[len_current_records - int(len(self.records))]
                            for record in new_records:
                                print(json.dumps(record, sort_keys=True, indent=4, default=str))

                            records_dict[shard['ShardId']] = self.records
                    else:
                        for record in self.records:
                            print(json.dumps(record, sort_keys=True, indent=4, default=str))
                        records_dict[shard['ShardId']] = self.records


if __name__ == '__main__':
    read_stream = readStream()
    read_stream.run()

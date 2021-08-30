import boto3

s3 = boto3.client('s3',
                  region_name="us-west-2")

# my_bucket = s3.Bucket('udacity-dend')
#
# for my_bucket_object in my_bucket.objects.all():
#     print(my_bucket_object)

result = s3.list_objects_v2(Bucket='udacity-dend', Prefix='log_data/2018/11/2018-11-0')
print(len(result['Contents']))

import os
import boto3
import json
import uuid

queue_name = os.environ.get('QUEUE_NAME')
message_group_id = os.environ.get('MSG_GROUP_ID')
is_debug = os.environ.get('IS_DEBUG')


def lambda_handler(event, context):
    queue_name = 'DevAICheatQ.fifo'
    message_group_id = 'groupID'
    is_debug = 'deduplicationID'
    sqs = boto3.resource('sqs')
    try:
        # キューの名前を指定してインスタンスを取得
        queue = sqs.get_queue_by_name(QueueName=queue_name)
    except:
        print('{} is Not Found.'.format(queue_name))

    # que message body
    body = {
        'Domain': 'xxxxxxxxxxx',
        'Sentdirectory': 'yyyymmdd-n',
        'Analysis': 'n',
        'filename': 'https://at-shared-documents.s3-ap-northeast-1.amazonaws.com/output_sample/output2.json',
        'snap': 'xxxxxxxxxxxxxxxxx',
        'requester': 'arn:aws:iam::597775291172:role/aaa-ryotaro-S3RekoTrans'}
    # メッセージをキューに送信
    response = queue.send_message(
        MessageBody = json.dumps(body),
        MessageDeduplicationId = str(uuid.uuid4()),
        MessageGroupId = message_group_id
    )
    print('send_message={0}'.format(response))
   

    ##########
    # Dequeue処理
    ##########
    # waitimesecondsを1以上に設定することでコスパよく、無駄なリクエストなくデキューが可能です
    # ここでは20secとしています。(Long Polling)
    if is_debug:
        res_messages = queue.receive_messages(
            AttributeNames=[message_group_id],
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1   # 小柳様のPyでは1を指定頂く想定です。1動画のみ処理する。
        )
        # 戻りはlist(sqs.Message)型
        print(res_messages)
        json_body = json.loads(res_messages[0].body)
        res_messages[0].delete()
        return json_body
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Message.body
#         for res_message in res_messages:
#             json_body = json.loads(res_message.body)
#             print(res_message.body)
#             que_object_url = json_body['objectUrl']
#             print('objectUrl:{0}'.format(que_object_url))
#             res_message.delete()
print("a")
json_body =  lambda_handler('event', 'context')

from datetime import datetime, timedelta, timezone
from boto3.dynamodb.conditions import Key

JST = timezone(timedelta(hours=+9), 'JST')
# tz指定は必須
now = datetime.now(JST)
# 文字列に変換
now_str = now.isoformat()

dynamodb = boto3.resource('dynamodb')
table    = dynamodb.Table('devCheatLog')

# (略)

# param in queue
objectUrlFromQueue = json_body['filename']
# DynamoDBへのquery処理実行
queryData = table.put_item(
    Item = {
        "objectUrl": objectUrlFromQueue, # Partition Keyのデータ
        "date": now_str, # Sort Keyのデータ
        "status": "pending"
    }
)

try:
    response = table.get_item(Key={'objectUrl': objectUrlFromQueue, 'date': now_str})
except ClientError as e:
    print(e.response['Error']['Message'])

response = table.update_item(
    Key={
        "objectUrl": objectUrlFromQueue, # Partition Keyのデータ
        "date": now_str,
    },
    UpdateExpression="set #st=:s, examName=:n, requester=:r",
    ExpressionAttributeNames= {
        '#st': 'status'  
    },
    ExpressionAttributeValues={
        ':s': 'succeeded',
        ':n': '社内資格試験',
        ':r': json_body['requester']
    }
)
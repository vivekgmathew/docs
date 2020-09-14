def lambda_handler(event, context):
    resp = sqs.receive_message(QueueUrl=queue_url,
                           AttributeNames=['All'],
                           MaxNumberOfMessages=10)
    try:
        messages = resp['Messages']
    except KeyError:
        print('No messages on the queue!')
        messages = []
    
    for msg in messages:
        print(msg)
        result_id = json.loads(msg['Body'])['alertResults'][0]['resultID']
        receipt_handle = msg["ReceiptHandle"]
        
        print(result_id)
        print(receipt_handle)
        
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        print("Message deleted")

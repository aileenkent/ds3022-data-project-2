import os
import boto3
import time
import requests

queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/sbx3sw"
sqs = boto3.client('sqs')
api_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/sbx3sw"

payload = requests.post(url).json

def initialize_queue():
    try:
        response = request.post(api_url)
        response.raise_for_status()
        payload = response.json()
        print("Queue initialized - 21 messages")
        return payload
    except requests.exceptions.RequestException as e:
        print(f"Error initializing queue: {e}")
        raise e

#def populate_queue_once():
#    resp = requests.post(api_url)
#    rasp.raise_for_status()

def delete_message(queue_url, receipt_handle):
    try:
        sqs.delete_message(QueueUrl

def get_messages(queue_url, expected_count=21, max_wait=1000):
    all_messages = []
    start_time = time.time
    try:
        while len(all_messages) < expected_count and (time.time() - start_time) < max_wait:
            try:
                response = sqs.receive_message(
                    QueueUrl=url,
                    MessageSystemAttributeNames=['All'],
                    MaxNumberOfMessages=10,
                    VisibilityTimeout=60,
                    MessageAttributeNames=['All'],
                    WaitTimeSeconds=30
                )
                messages = response.get("Messages", [])
                if messages:
                    for msg in messages:
                        all_messages.append(msg)
                        print(f"Recieved message {len(all_messages)} / {expected_count}")
                        try:
                            sqs.delet_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=msg["ReceiptHandle"]
                            )
                            print("Deleted message from queue")
                        except (BotoCoreError, ClientError) as delete_error:
                            print(f"Error deleting message: {delete_error}")
                else:
                    print("Waiting for delayed messages")
                time.sleep(5)
            except (BotoCoreError, ClientError) as sqs_error:
                print(f"Error communicating with SQS: {sqs_error}")
                time.sleep(10)
        if len(all_messages) < expected_count:
            print(f"Only recieved {len(all_messages)} of {expected_count} messages after {max_waits}s")
        else:
            print("All 21 messages recieved")
    except Exception as e:
        print(f"Error while polling messages: {e}")
        return e
    return all_messages


def get_queue_attributes(queue_url, expected_count=21):
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        attrs = response.get('Attributes', {})

        visible = int(attrs.get('ApproximateNumberOfMessages', 0))
        not_visible = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
        total = visible + not_visible + delayed

        print(f"Visible Messages: {visible}")
        print(f"Not visible messages: {not_visible}")
        print(f"delayed messages: {delayed}")
        print(f"total messages: {total}/{expected_count}")

        return {
            "visible": visible,
            "not_visibile": not_visible,
            "delayed": delayed,
            "total": total,
        }

#        print(f"Response: {response}")
    except Exception as e:
        print(f"Error getting queue attributes: {e}")
        return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}

def wait_for_all_messages(queue_url, expected_count=21, timeout=1000, check_interval=30):
    start = time.time()
    while (time.time() - start) < timeout:
        counts = get_queue_attribues(queue_url, expected_count)
        if counts["total"] >= expected_count:
            print("all expected messages are now in the queue")
            return True
        else:
            print(f"Waiting for all messages ({counts['total']}/{expected_count}) total")
            time.sleep(check_interval)
    print("Timeout waiting for all messages")
    return False

def receive_and_process_messages(queue_url, expected_count=21):
    processed_messages = []

    while len(processed_messages) < expected_count:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MessageSystemAttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=60
            )

            messages = response.get("Messages", [])
            if not messages:
                print("No visible messages yet")
                time.sleep(5)
                continue

            for msg in messages:
                attrs = msg.get("MessageAttributes", {})

                order_no = attrs.get("order_no", {}).get("StringValue", "Unknown")
                word = attrs.get("word", {}).get("StringValue", "Unknown")

                processed_messages.append({
                    "order_no": order_no,
                    "word": word
                })

                print(f"Received message: {order_no}, word: {word}")

                try:
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    print(f"Deleted message (order_no: {order_no})")
                except Exception as del_err:
                    print(f"Failed to delete message (order_no: {order_no}): {del_err}")

        except (BotoCoreError, ClientError) as sqs_error:
            print(f"SQS communication error: {sqs_error}")
            time.sleep(10)
        except Exception as e:
            print(f"Unexpected error receiving messages: {e}")
            time.sleep(10)

    print(f"All {len(processed_messages)} messages processed and deleted")
    return processed_messages

def run_pipeline():
    print("Starting pipeline")

    ready = wait_for_all_messages(queue_url, expected_count=21)

    if ready:
        data = receive_and_process_messages(queue_url, expected_count=21)
        print("Final processed message data:")
        for item in data:
            print(f"Order No: {item['order_no']} | Word: {item['word']}")
    else:
        print("not all messages arrived before timeout, exiting pipeline")


if __name__ = "__main__":
    initialize_queue()
    try:
        messages = get_all_messages(queue_url)
        print(f"{len(messages)} messages retrieved")
    except Exception as e:
        print(f"pipeline failed: {e}")
#    get_queue_atributes(url)


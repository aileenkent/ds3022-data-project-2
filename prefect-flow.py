import os
import boto3
import time
import requests

queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/sbx3sw"
sqs = boto3.client('sqs')
api_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/sbx3sw"

def initialize_queue():
    try:
        response = requests.post(api_url)
        if response.status_code != 200:
            print(f"Init queue failed ({response.status_code}): {response.text}")
        response.raise_for_status()
        payload = response.json()
        print("Queue initialized - 21 messages")
        return payload
    except requests.exceptions.RequestException as e:
        print(f"Error initializing queue: {e}")
        raise e

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

        print(f"Visible Messages: {visible}, Not visible messages: {not_visible}, delayed messages: {delayed}, total messages: {total}/{expected_count}")

        return {
            "visible": visible,
            "not_visibile": not_visible,
            "delayed": delayed,
            "total": total,
        }

    except Exception as e:
        print(f"Error getting queue attributes: {e}")
        return {"visible": 0, "not_visible": 0, "delayed": 0, "total": 0}

def wait_for_all_messages(queue_url, expected_count=21, timeout=1000, check_interval=30):
    start = time.time()
    while (time.time() - start) < timeout:
        counts = get_queue_attributes(queue_url, expected_count)
        if counts["total"] >= expected_count:
            print("all expected messages are now in the queue")
            return True
        else:
            print(f"Waiting for all messages ({counts['total']}/{expected_count})")
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

def send_solution(uvaid, phrase, platform="prefect"):
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try:
        response = sqs.send_message(
            QueueUrl=submit_url,
            MessageBody="Solution submission",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            print("Solution successfully submitted to SQS!")
        else:
            print(f"SQS returned status code {status_code}")

        print(f"Response: {response}")
        return response

    except Exception as e:
        print(f"Error sending solution: {e}")
        raise

def run_pipeline():
    print("Starting pipeline")
    initialize_queue()

    ready =  wait_for_all_messages(queue_url, expected_count=21)
    if not ready:
        print("Not all messages arrived before timeout, exiting pipeline")
        return

    data = receive_and_process_messages(queue_url, expected_count=21)

    phrase_parts = sorted(data, key=lambda x: int(x['order_no']))
    phrase = " ".join([p["word"] for p in phrase_parts])
    print(f"\nFinal completed phrase:\n{phrase}\n")

    send_solution(uvaid="sbx3sw", phrase=phrase, platform="prefect")


if __name__ == "__main__":
    run_pipeline()


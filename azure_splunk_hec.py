import azure.functions as func
import logging
import json
from azure.storage.blob import ContainerClient
import os
import gzip
from io import BytesIO
import json
import requests

app = func.FunctionApp()

QUEUE_NAME = os.environ["AZURE_QUEUE_NAME"]


@app.queue_trigger(
    arg_name="msg",
    queue_name=QUEUE_NAME,
    connection="AzureWebJobsStorage",
)
async def queue_trigger(msg: func.QueueMessage):
    logging.info(
        "Python Queue trigger processed a message: %s", msg.get_body().decode("utf-8")
    )

    try:
        result = {"id": msg.id, "body": msg.get_body()}
        # body should be a list of dicts, where each dict has link, bucket_name, sqs_message_id
        body = json.loads(result["body"].decode("ascii").replace("'", '"'))

        msgType = body["type"]
        if msgType == "Microsoft.Storage.BlobCreated":
            urlToProcess = body["data"]["url"]
            azure_client = AzureBlobStorage(urlToProcess)
            # send to Splunk via HEC
            fileName = urlToProcess.split("/")[-1]
            events = azure_client.load(fileName)
            logging.info("Events processed in %s is  %s", fileName, len(events))
            hec_client = SplunkHECClient()
            if len(events) < hec_client.eps:
                hec_client.send_to_splunk_hec(events)
            else:
                # send a batch of max 50 events per POST call
                # ref: https://conf.splunk.com/files/2017/slides/measuring-hec-performance-for-fun-and-profit.pdf
                start = 0
                for _ in range(start, len(events), 50):
                    hec_client.send_to_splunk_hec(events[start : start + 50])
                    start += 50
    except Exception as e:
        logging.error(f"Exception while processing event from queue {e}")


class SplunkHECClient:
    def __init__(self):
        self.splunk_fqdn = os.environ.get(
            "SPLUNK_URL", "ec2-52-89-97-130.us-west-2.compute.amazonaws.com"
        )
        self.splunk_port = os.environ.get("SPLUNK_PORT", "8088")
        self.hec_token = os.environ.get(
            "SPLUNK_HEC_TOKEN", "6c6c7cd0-508d-4c9f-aee9-1c305d685a61"
        )
        self.hec_url = (
            f"http://{self.splunk_fqdn}:{self.splunk_port}/services/collector/event"
        )
        self.headers = {
            "Authorization": f"Splunk {self.hec_token}",
            "Connection": "keep-alive",
            "keep-alive": "timeout=5, max=30",
        }
        self.sourcetype = os.environ.get("sourcetype", "illumio:pce")
        self.eps = int(os.environ.get("EVENTS_PER_SECOND", 50))

    def build_payload(self, data):
        event_arr = []
        for event in data:
            payload = {"sourcetype": self.sourcetype, "event": event}
            event_arr.append(json.dumps(payload))
        return event_arr

    def send_to_splunk_hec(self, data):
        """Sends data to Splunk HEC."""

        try:
            data_ = self.build_payload(data)
            data_ = "\n".join(event for event in data_)
            print(data_)
            response = requests.post(self.hec_url, headers=self.headers, data=data_)
            response.raise_for_status()  # Raise an exception for bad status codes
            print("Data sent successfully. No of events sent is %s", len(data))
        except requests.exceptions.RequestException as e:
            print(f"Error sending data to Splunk HEC: {e}")


class AzureBlobStorage:
    def __init__(self, blob_url):
        storage_account = os.environ.get("AzureWebJobsStorage")

        # determine whether url is audit event or flow summaries
        if "auditable" in blob_url:
            container = os.environ.get("StorageContainer", "auditable-pcelogs")
        else:
            container = os.environ.get("StorageContainer", "networktraffic-pcelogs")
        self.container_client = ContainerClient.from_connection_string(
            conn_str=storage_account, container_name=container
        )

    def load(self, file_name: str) -> dict:
        blob_client = self.container_client.get_blob_client(blob=file_name)
        try:
            event_data = []
            blob_data = blob_client.download_blob().readall()

            with gzip.GzipFile(fileobj=BytesIO(blob_data)) as gz:
                decompressed_data = gz.read()

                for line in decompressed_data.decode("utf-8").splitlines():
                    try:
                        if line:
                            json_obj = json.loads(line)
                            event_data.append(json_obj)
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse line: {line}")
                        print(f"Error: {e}")
            logging.info("Count of events processed is %s", len(event_data))
            return event_data
        except Exception as e:
            logging.error(f"Error reading blob {file_name}: {str(e)}")
            return {}

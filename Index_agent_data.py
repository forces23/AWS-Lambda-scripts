import logging
import os
import boto3
import json
from flatten_json import flatten

from scripts_utils import decode_data, timestamp_reformat, get_object_key

# set the logging level here 
log = logging.getLogger()
log.setLevel(
   logging.INFO
   #logging.DEBUG
   )

# Inside AWS Environment
BUCKET = os.environ['S3_BUCKET_NAME']
PREFIX = os.environ['S3_DATA_PREFIX']

# SKIP_AGENT_HEARTBEAT_EVENTS is set in the environment vaiable in aws
# if it sees HEART_BEAT in the record and SKIP_AGENT_HEARTBEAT_EVENTS is set to TRUE in Env Variables in aws then it will skip this lines of data
if os.environ['SKIP_AGENT_HEARTBEAT_EVENTS'] == "true":
   SKIP_AGENT_HEARTBEAT_EVENT = True
else: SKIP_AGENT_HEARTBEAT_EVENT = False

s3 = boto3.client('s3')

# Puts each record as an object into a specified s3 bucked
def lambda_handler(event, context):
   log.info("index_agent_data lambda function started ...")
   log.debug(f"--------- event -----------\n"
            +"{json.dumps(event)}")
   
   record_data = event["Records"]
   
   for data in record_data:
      
      # decodes the record and flattens it
      decoded_record = decode_data(flatten(data))
      
      log.debug(f"--------- decoded_record -------------\n"
                  +"{decoded_record}")
      
      # If the event type is set to heartbeat then it will skip that record 
      log.debug(f"Event Type = {decoded_record['EventType']}")
      if SKIP_AGENT_HEARTBEAT_EVENT and (decoded_record['EventType'] == 'HEART_BEAT'):
         continue
      
      # Reformats the timestamp and flattens the record 
      reformatted_timestamp = timestamp_reformat(flatten(decoded_record))
      
      try:
         # Creates the Json file and stores it in the correct area in the S3 bucket
         file_data = json.dumps(reformatted_timestamp)
         log.debug(f"Bucket={BUCKET}")
         s3.put_object(Body=file_data, Bucket=BUCKET, Key=get_object_key(decoded_record,PREFIX))
      except Exception as e:
         log.info("There was a Problem inserting into the S3 bucket "
               +"\nError:{}".format(str(e))
         )
      
   log.info("index_agent_data lambda function completed ...")
   print("...DONE")    



import json 
#import environ 
import os
import boto3
from datetime import datetime
import logging
from flatten_json import flatten
from scripts_utils import decode_data, timestamp_reformat, get_contact_attributes_from_record, get_object_key

# set the logging level here 
log = logging.getLogger()
log.setLevel(
    logging.INFO
    #logging.DEBUG
    )

# these 3 below are set in the enviroment variables section within lambda function 
BUCKET =  os.environ['S3_BUCKET_NAME']
CONTACT_PREFIX = os.environ['S3_DATA_PREFIX_CONTACT']
ATTRIBUTE_PREFIX = os.environ['S3_DATA_PREFIX_ATTRIBUTES']

S3 = boto3.client('s3')

# event that gets passed in is the record aws kinesis data
def lambda_handler(event, context):
    log.info("index_contact_s3 lambda function started ...")
    log.debug(f"--------- event -----------\n"
             +"{json.dumps(event)}\n")
    
    record_data = event["Records"]
    for data in record_data:
        log.debug(f"--------- record data -----------\n"
                 +"{data}\n")
        
        # flattens the record 
        flatten_record = flatten(data)
        
        log.debug(f"---------flatten record -----------\n"
                 +"{flatten_record}\n")
        
        # decodes the record after it is flattened
        record = decode_data(flatten_record)
        
        log.debug(f"--------- decoded flatten record -----------\n"
                 +"{record}\n")
        
        # stores contact data and attribute data into seperate variables 
        contact_data, attribute_data = get_contact_attributes_from_record(record)
        
        # goes into contact data and grabs/reformats the timestamp
        contact_data_timestamp = timestamp_reformat(contact_data)
        
        # Checks to make sure that the Contact Data gets inserted into the S3 bucket correctly 
        try:
            # stores the contact data in the S3 bucket
            contact_object_key = get_object_key(record, CONTACT_PREFIX, False)
            S3.put_object(Body=json.dumps(contact_data_timestamp), Bucket=BUCKET, Key=contact_object_key)
            
        except Exception as e:
            log.info("There was a problem inserting Contact Data into the S3 bucket "
               +"\nError:{}".format(str(e))
            )
        
        # Checks to make sure that the Attribute Data gets inserted into the S3 bucket correctly 
        try:
            for data in attribute_data:
                attribute_data_timestamp= timestamp_reformat(data)
                
                attribute_object_key = get_object_key(data, ATTRIBUTE_PREFIX, True)
                S3.put_object(Body=json.dumps(attribute_data_timestamp), Bucket=BUCKET, Key=attribute_object_key)
                
        except Exception as e:
            log.info("There was a problem inserting Attribute Data into the S3 bucket "
               +"\nError:{}".format(str(e))
            )
            
    log.info("index_contact_s3 lambda function completed")
    print("...DONE")    



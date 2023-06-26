from datetime import datetime
from datetime import timezone
import uuid
from flatten_json import flatten
import base64
import json
import logging

# set the logging level here 
log = logging.getLogger()
log.setLevel(
    logging.INFO
    #logging.DEBUG
    )

SUFFIX = ".json"

# function that takes in a dict(record) and checks whether it has attribute name1 or name2
# if it has either you want to check the timestamp and take the last character off the string
def get_object_key(data, prefix, contact_attribute=False):
    log.info("Getting the object key ready ... ")
    # Sets the customer call date 
    if data.get("InitiationTimestamp") or data.get("EventTimestamp") is not None:
        if "InitiationTimestamp" in data:
            timeStamp = data["InitiationTimestamp"]
        else:
            timeStamp = data["EventTimestamp"]
        if timeStamp.endswith("Z"):
            timeStamp = timeStamp.rstrip(timeStamp[-1])
        customer_call_date = datetime.fromisoformat(timeStamp)
    else:
        customer_call_date = datetime.now(timezone.utc)
    
    # Sets the Id for record
    if data.get("ContactId") or data.get("EventId") is not None:
        if "ContactId" in data:
            data_key = data["ContactId"]
        else:
            data_key = data["EventId"]
    else: 
        # make a UUID using an MD5 hash of a namespace UUID and a name
        data_key = uuid.uuid5(uuid.NAMESPACE_DNS, "Amazon Web Services")

    if contact_attribute:
        data_key = data["ContactId"] + data["attributename"]

    file_name = f"{data_key}"

    return get_object_key_from_dateTime(prefix, customer_call_date, file_name)

# creates a object key using the timestamp, it does this by splitting the year month and day up 
# DONT ADD IN THE .json IN WITH filename param
def get_object_key_from_dateTime(prefix, customer_call_date, file_name):
    log.info("Setting the key with ../year/month/day ... ")
    prefix = f"{prefix}/year={customer_call_date.year:02d}/month={customer_call_date.month:02d}/day={customer_call_date.day:02d}"
    object_key = f"{prefix}/{file_name}{SUFFIX}"
    return object_key
   
# refromats the timestamp thats located within the record
def timestamp_reformat(data):
    log.info("Reformatting Timestamp ... ")
    for key in data:
        if "Timestamp" in key and data[key] != None:
            timestamp_string = data[key]
            if timestamp_string.casefold().endswith('z'):
                timestamp_string = timestamp_string.rstrip(timestamp_string[-1])
            formatted_timestamp = datetime.fromisoformat(timestamp_string).strftime("%y-%m-%d %H:%M:%S")
            data[key] = formatted_timestamp
                
    return data

# decodes the data
def decode_data(data):
    log.info("Decoding Data ... ")
    decoded_bytes = base64.b64decode(data['kinesis_data'])
    decoded_string = decoded_bytes.decode("utf-8")
    decoded_json_data = json.loads(decoded_string)
    return decoded_json_data

def set_data_keys_lowercase(data):
    temp_data = {}
    for key in data:
        temp_data.update({key.casefold(): data[key]})
    return temp_data

# seperates the contact data and the attribute data 
def get_contact_attributes_from_record(data):
    log.info("Getting contact data and attribute data from the record ... ")
    temp_data = {}
    contact_data = {}
    attributes_data = []
    if data.get("contactid") or data.get("ContactId") is not None:
        id = data.get("contactid") or data.get("ContactId")
    else: id = None
    
    for key in data:
        if 'attributes_' in key:
            temp_data.update({"contactid" : id})
            temp_data.update({"attribute_value": data[key]})
            attributes_data.update({'attributename': key.split()[-1]})
            temp_data = {}
        else:
            contact_data.update({key:data[key]})

    return contact_data,attributes_data

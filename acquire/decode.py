import json
import xml.etree.ElementTree as ET

def decode(body, headers):
    return [{'key': x['message_id'], 'value': x} for x in json.loads(body)['messages']]

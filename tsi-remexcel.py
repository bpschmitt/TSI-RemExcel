import csv
import json
import logging
import sys
import time
import concurrent.futures as cf
from requests_futures.sessions import FuturesSession

with open('param.json') as json_data:
    parms = json.load(json_data)

threeyears = 94608000

def getCSVHeader():
    mappings={}
    f = open(parms["file"])
    reader = csv.reader(f)
    header = reader.__next__()

    for i, val in enumerate(header):
        mappings[val] = i

    return mappings

def getItem(mappings,value):

    text = ""
    #print(mappings)
    #print(value)
    for k,v in mappings.items():
        #print("value: " + str(value) + " = v: " + str(v))
        if(int(value) == int(v)):
            text = k
            #print(text)

    return text

def createEventJSON():

    eventList=[]
    header = getCSVHeader()
    f = open(parms["file"],encoding='Windows-1252')
    reader = csv.reader(f)
    reader.__next__()

    for events in reader:
        try:
            event = {
                "source": parms["sourcesender"],
                "sender": parms["sourcesender"],
                "fingerprintFields": parms["fingerprintfields"],
                "title": events[header["DESCRIPTION"]],
                "status": getItem(parms["status"],events[header["STATUS"]]),
                "createdAt": int(events[header["SUBMIT_DATE"]]) + threeyears,
                "eventClass": "Incident",
                "properties": {
                    "urgency": getItem(parms["urgency"],events[header["URGENCY"]]),
                    "impact": getItem(parms["impact"],events[header["IMPACT"]]),
                    "incident_id": events[header["INCIDENT_NUMBER"]],
                    "owner_group": events[header["OWNER_GROUP"]],
                    "assigned_group": events[header["ASSIGNED_GROUP"]],
                    "organization": events[header["ORGANIZATION"]],
                    "closed_date": events[header["CLOSED_DATE"]],
                    "city": events[header["CITY"]],
                    "site": events[header["SITE"]],
                    "company": events[header["COMPANY"]],
                    "assignee": events[header["ASSIGNEE"]],
                    "app_id": parms["app_id"]
                },
                "tags": [parms["app_id"]]
            }
            event = json.dumps(event)
            eventList.append(event)
        except:
            pass

    return eventList

def sendAsyncEvents(events):

    logging.basicConfig(
        stream=sys.stderr, level=logging.INFO,
        format='%(relativeCreated)s %(message)s',
    )

    session = FuturesSession()
    futures = {}

    logging.info('start')

    counter=0
    for event in events:
        while counter < parms['chunksize']:
            future = session.post(parms['url'],data=event,headers=parms['headers'],auth=(parms['email'],parms['apikey']))
            futures[future] = event
            print(event)
            counter+=1
        counter=0
        time.sleep(parms['sleeptime'])

    for future in cf.as_completed(futures):
        res = future.result()
        logging.info(
            "event=%s, %s, %s",
            futures[future],
            res,
            len(res.text)
        )

    logging.info('done!')

events = createEventJSON()
sendAsyncEvents(events)


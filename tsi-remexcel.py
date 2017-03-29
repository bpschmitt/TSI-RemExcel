import csv
import json
import logging
import sys
import time
import datetime
import concurrent.futures as cf
from requests_futures.sessions import FuturesSession

with open('param_secret.json') as json_data:
    parms = json.load(json_data)

def getCSVHeader():
    mappings = {}
    f = open(parms["file"])
    reader = csv.reader(f)
    header = reader.__next__()

    for i, val in enumerate(header):
        mappings[val] = i

    return mappings


def getItem(mappings, value):
    text = ""
    # print(mappings)
    # print(value)
    for k, v in mappings.items():
        # print("value: " + str(value) + " = v: " + str(v))
        if (int(value) == int(v)):
            text = k
            # print(text)

    return text

def convertTS(ts):
    return datetime.datetime.fromtimestamp(int(ts)).strftime('%m-%d-%Y %H:%M:%S')


def createEventJSON():
    eventList = []
    header = getCSVHeader()
    f = open(parms["file"], encoding='Windows-1252')
    reader = csv.reader(f)
    reader.__next__()

    for events in reader:
        try:
            event = {
                "source": parms["sourcesender"],
                "sender": parms["sourcesender"],
                "fingerprintFields": parms["fingerprintfields"],
                "title": events[header["DESCRIPTION"]],
                "status": getItem(parms["status"], events[header["STATUS"]]),
                "createdAt": int(events[header["SUBMIT_DATE"]])+parms['shift'],
                "eventClass": "Incident",
                "properties": {
                    "app_id": parms["app_id"],
                    "assigned_group": events[header["ASSIGNED_GROUP"]],
                    "assigned_support_company": events[header["ASSIGNED_SUPPORT_COMPANY"]],
                    "assigned_support_org": events[header["ASSIGNED_SUPPORT_ORGANIZATION"]],
                    "assignee": events[header["ASSIGNEE"]],
                    "city": events[header["CITY"]],
                    "closed_date": convertTS(int(events[header["CLOSED_DATE"]])+parms['shift']),
                    "company": events[header["COMPANY"]],
                    "country": events[header["COUNTRY"]],
                    "department": events[header["DEPARTMENT"]],
                    "impact": getItem(parms["impact"], events[header["IMPACT"]]),
                    "last_modified_date": convertTS(int(events[header["LAST_MODIFIED_DATE"]])+parms['shift']),
                    "last_resolved_date": convertTS(int(events[header["LAST_RESOLVED_DATE"]])+parms['shift']),
                    "incident_id": events[header["INCIDENT_NUMBER"]],
                    "organization": events[header["ORGANIZATION"]],
                    "owner_group": events[header["OWNER_GROUP"]],
                    "reported_date": convertTS(int(events[header["REPORTED_DATE"]])+parms['shift']),
                    "resolution_category": events[header["RESOLUTION_CATEGORY"]],
                    "site": events[header["SITE"]],
                    "state_province": events[header["STATE_PROVINCE"]],
                    "submit_date": convertTS(int(events[header["SUBMIT_DATE"]])+parms['shift']),
                    "urgency": getItem(parms["urgency"], events[header["URGENCY"]]),
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

    bufcounter = 0
    eventcounter = 0
    while (eventcounter < len(events)):
        if (bufcounter == parms['chunksize']):
            print('sleeping...')
            time.sleep(parms['sleeptime'])
            bufcounter = 0
        else:
            future = session.post(parms['url'], data=events[eventcounter], headers=parms['headers'], auth=(parms['email'], parms['apikey']))
            futures[future] = events[eventcounter]
            print(str(eventcounter) + ": " + events[eventcounter])
            eventcounter += 1
            bufcounter += 1

    """ for event in events:
        future = session.post(parms['url'],data=event,headers=parms['headers'],auth=(parms['email'],parms['apikey']))
        futures[future] = event
        print(event)
        counter+=1

        time.sleep(parms['sleeptime']) """

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

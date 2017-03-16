import csv
import json
import requests
from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor

session = FuturesSession(executor=ThreadPoolExecutor(max_workers=50))
url = "https://api.truesight.bmc.com/v1/events"
headers = {'Content-type': 'application/json'}
threeyears = 94608000

with open('param.json') as json_data:
    parms = json.load(json_data)


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
                "status": "Open",
                "createdAt": int(events[header["SUBMIT_DATE"]]) + threeyears,
                "properties": {
                    "title": events[header["DESCRIPTION"]],
                    "eventClass": "Incident",
                    "status": getItem(parms["status"],events[header["STATUS"]]),
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

            print(event)
        except:
            pass

createEventJSON()

""" data_json = json.dumps(data)
print(data_json)

myResponse = requests.post(url, data=data_json, headers=headers, auth=(parms["email"], parms["apikey"]))
print(str(myResponse.status_code) + " - " + str(myResponse.reason)) """

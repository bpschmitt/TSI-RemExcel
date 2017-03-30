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
    if ts != "":
        ts = int(ts) + parms['timeshift']
        return datetime.datetime.fromtimestamp(ts).strftime('%m-%d-%Y %H:%M:%S')
    else:
        return ts


def createEventJSON():
    eventList = []
    header = getCSVHeader()
    f = open(parms["file"], encoding='Windows-1252')
    reader = csv.reader(f)
    reader.__next__()

    for events in reader:
        try:
            if parms['file_type'] == "I":
                event = {
                    "source": parms["sourcesender"],
                    "sender": parms["sourcesender"],
                    "fingerprintFields": parms["fingerprintfields"],
                    "title": events[header["DESCRIPTION"]],
                    "status": getItem(parms["status"], events[header["STATUS"]]),
                    "createdAt": int(events[header["SUBMIT_DATE"]])+parms['timeshift'],
                    "eventClass": "Incident",
                    "properties": {
                        "app_id": parms["app_id"],
                        "assigned_group": events[header["ASSIGNED_GROUP"]],
                        "assigned_support_company": events[header["ASSIGNED_SUPPORT_COMPANY"]],
                        "assigned_support_org": events[header["ASSIGNED_SUPPORT_ORGANIZATION"]],
                        "assignee": events[header["ASSIGNEE"]],
                        "categorization_tier1": events[header["CATEGORIZATION_TIER_1"]],
                        "categorization_tier2": events[header["CATEGORIZATION_TIER_2"]],
                        "categorization_tier3": events[header["CATEGORIZATION_TIER_3"]],
                        "city": events[header["CITY"]],
                        "closed_date": convertTS(events[header["CLOSED_DATE"]]),
                        "closure_manufacturer": events[header["CLOSURE_MANUFACTURER"]],
                        "closure_prod_cat_tier1": events[header["CLOSURE_PRODUCT_CATEGORY_TIER1"]],
                        "closure_prod_cat_tier2": events[header["CLOSURE_PRODUCT_CATEGORY_TIER2"]],
                        "closure_prod_cat_tier3": events[header["CLOSURE_PRODUCT_CATEGORY_TIER3"]],
                        "company": events[header["COMPANY"]],
                        "country": events[header["COUNTRY"]],
                        "department": events[header["DEPARTMENT"]],
                        "first_name": events[header["FIRST_NAME"]],
                        "last_name": events[header["LAST_NAME"]],
                        "impact": getItem(parms["impact"], events[header["IMPACT"]]),
                        "last_modified_date": convertTS(events[header["LAST_MODIFIED_DATE"]]),
                        "last_resolved_date": convertTS(events[header["LAST_RESOLVED_DATE"]]),
                        "incident_id": events[header["INCIDENT_NUMBER"]],
                        "organization": events[header["ORGANIZATION"]],
                        "owner_group": events[header["OWNER_GROUP"]],
                        "prod_cat_tier1": events[header["PRODUCT_CATEGORIZATION_TIER_1"]],
                        "prod_cat_tier2": events[header["PRODUCT_CATEGORIZATION_TIER_2"]],
                        "prod_cat_tier3": events[header["PRODUCT_CATEGORIZATION_TIER_3"]],
                        "reported_date": convertTS(events[header["REPORTED_DATE"]]),
                        "resolution_category": events[header["RESOLUTION_CATEGORY"]],
                        #"resolution_cat_tier1": events[header["RESOLUTION_CATEGORY_TIER_1"]],
                        "resolution_cat_tier2": events[header["RESOLUTION_CATEGORY_TIER_2"]],
                        "resolution_cat_tier3": events[header["RESOLUTION_CATEGORY_TIER_3"]],
                        "site": events[header["SITE"]],
                        "site_group": events[header["SITE_GROUP"]],
                        "state_province": events[header["STATE_PROVINCE"]],
                        "submit_date": convertTS(events[header["SUBMIT_DATE"]]),
                        "urgency": getItem(parms["urgency"], events[header["URGENCY"]]),
                    },
                    "tags": [parms["app_id"]]
                }
            elif parms['file_type'] == "P":
                event = {
                    "source": parms["sourcesender"],
                    "sender": parms["sourcesender"],
                    "fingerprintFields": parms["fingerprintfields"],
                    "title": events[header["DESCRIPTION"]],
                    "status": events[header["STAGECONDITION"]],
                    "createdAt": int(events[header["SUBMIT_DATE"]]) + parms['timeshift'],
                    "eventClass": "Problem",
                    "properties": {
                        "app_id": parms["app_id"],
                        "assigned_group": events[header["ASSIGNED_GROUP"]],
                        "assigned_group_pblm_mgr": events[header["ASSIGNED_GROUP_PBLM_MGR"]],
                        "assigned_support_company": events[header["ASSIGNED_SUPPORT_COMPANY"]],
                        "assigned_support_org": events[header["ASSIGNED_SUPPORT_ORGANIZATION"]],
                        "assignee": events[header["ASSIGNEE"]],
                        "assignee_pblm_mgr": events[header["ASSIGNEE_PBLM_MGR"]],
                        "categorization_tier1": events[header["CATEGORIZATION_TIER_1"]],
                        "categorization_tier2": events[header["CATEGORIZATION_TIER_2"]],
                        "categorization_tier3": events[header["CATEGORIZATION_TIER_3"]],
                        "closed_date": convertTS(events[header["CLOSED_DATE"]]),
                        "company": events[header["COMPANY"]],
                        "contact_company": events[header["CONTACT_COMPANY"]],
                        "department": events[header["DEPARTMENT"]],
                        "first_name": events[header["FIRST_NAME"]],
                        "first_reported_on": convertTS(events[header["FIRST_REPORTED_ON"]]),
                        "last_name": events[header["LAST_NAME"]],
                        "impact": getItem(parms["impact"], events[header["IMPACT"]]),
                        "last_modified_date": convertTS(events[header["LAST_MODIFIED_DATE"]]),
                        "priority": events[header["PRIORITY"]],
                        "prod_cat_tier1": events[header["PRODUCT_CATEGORIZATION_TIER_1"]],
                        "prod_cat_tier2": events[header["PRODUCT_CATEGORIZATION_TIER_2"]],
                        "prod_cat_tier3": events[header["PRODUCT_CATEGORIZATION_TIER_3"]],
                        "region": events[header["REGION"]],
                        "serviceci": events[header["SERVICECI"]],
                        "serviceci_class": events[header["SERVICECI_CLASS"]],
                        "site": events[header["SITE"]],
                        "site_group": events[header["SITE_GROUP"]],
                        "stage_condition": events[header["STAGECONDITION"]],
                        "submit_date": convertTS(events[header["SUBMIT_DATE"]]),
                        "support_company_pblm_mgr": events[header["SUPPORT_COMPANY_PBLM_MGR"]],
                        "support_group_name_requestor": events[header["SUPPORT_GROUP_NAME_REQUESTER"]],
                        "support_organization_requestor": events[header["SUPPORT_ORGANIZATION_REQUESTOR"]],
                        "urgency": getItem(parms["urgency"], events[header["URGENCY"]]),
                    },
                    "tags": [parms["app_id"]]
                }
            elif parms['file_type'] == "C":
                event = {
                    "source": parms["sourcesender"],
                    "sender": parms["sourcesender"],
                    "fingerprintFields": parms["fingerprintfields"],
                    "title": events[header["DESCRIPTION2"]],
                    "status": getItem(parms["change_request_status"], events[header["CHANGE_REQUEST_STATUS"]]),
                    "createdAt": int(events[header["SUBMIT_DATE"]]),
                    "eventClass": "Change",
                    "properties": {
                        "app_id": parms["app_id"],
                        "company": events[header["COMPANY"]],
                        "customer_company": events[header["CUSTOMER_COMPANY"]],
                        "customer_department": events[header["CUSTOMER_DEPARTMENT"]],
                        "customer_first_name": events[header["CUSTOMER_FIRST_NAME"]],
                        "customer_last_name": events[header["CUSTOMER_LAST_NAME"]],
                        "customer_organization": events[header["CUSTOMER_ORGANIZATION"]],
                        "department": events[header["DEPARTMENT"]],
                        "first_name": events[header["FIRST_NAME"]],
                        "last_name": events[header["LAST_NAME"]],
                        "impact": getItem(parms["impact"], events[header["IMPACT"]]),
                        "last_modified_date": convertTS(events[header["LAST_MODIFIED_DATE"]]),
                        "organization": events[header["ORGANIZATION"]],
                        "requested_start_date": convertTS(events[header["REQUESTED_START_DATE"]]),
                        "scheduled_start_date": convertTS(events[header["SCHEDULED_START_DATE"]]),
                        "site_group": events[header["SITE_GROUP"]],
                        "submitter": events[header["SUBMITTER"]],
                        "submit_date": convertTS(events[header["SUBMIT_DATE"]]),
                        "support_group_name": events[header["SUPPORT_GROUP_NAME"]],
                        "support_group_name2": events[header["SUPPORT_GROUP_NAME2"]],
                        "support_organization": events[header["SUPPORT_ORGANIZATION"]],
                        "urgency": getItem(parms["urgency"], events[header["URGENCY"]]),
                    },
                    "tags": [parms["app_id"]]
                }
            else:
                print("Please specify the proper file type in param.json")
                exit(1)

            event = json.dumps(event)
            eventList.append(event)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments: {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            pass

    return eventList


def sendAsyncEvents(events):
    logging.basicConfig(
        stream=sys.stderr, level=logging.INFO,
        format='%(relativeCreated)s %(message)s',
    )

    session = FuturesSession()
    futures = {}

    logging.info('start!')

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

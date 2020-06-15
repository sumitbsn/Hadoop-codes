#!/usr/bin/python

###Ansible: MySql Replication Alert
## 0 */1 * * * root /root/mysql-replication/MySql-Replication-Alert.sh

## *******************************************************************************************
##  killLongRunningImpalaQueries.py
##
##  Kills Long Running Impala Queries
##
##  Usage: ./killLongRunningImpalaQueries.py  queryRunningSeconds [KILL]
##
##    Set queryRunningSeconds to the threshold considered "too long"
##    for an Impala query to run, so that queries that have been running
##    longer than that will be identifed as queries to be killed
##
##    The second argument "KILL" is optional
##    Without this argument, no queries will actually be killed, instead a list
##    of queries that are identified as running too long will just be printed to the console
##    If the argument "KILL" is provided a cancel command will be issues for each selcted query
##
##    CM versions <= 5.4 require Full Administrator role to cancel Impala queries
##
##    Set the CM URL, Cluster Name, login and password in the settings below
##
##    This script assumes there is only a single Impala service per cluster
##
## *******************************************************************************************


## ** imports *******************************
import smtplib
import os, sys
from datetime import datetime, timedelta
from cm_api.api_client import ApiResource
import ssl

if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
  ssl._create_default_https_context = ssl._create_unverified_context

## ** Settings ******************************

## Cloudera Manager Host
cm_host = "cmurl"
cm_port = "7183"

## Cloudera Manager login with Full Administrator role
cm_login = "impala-qmonitor"

## Cloudera Manager password
cm_password = "pass"

## Cluster Name
cluster_name = "cluster"

cm_api_version = "19"
## *****************************************

fmt = '%Y-%m-%d %H:%M:%S %Z'

def send_email(user, recipient, subject, body):


    user_email = user
#    user_pwd = pwd
    FROM = user
    TO = recipient if type(recipient) is list else [recipient]
    SUBJECT = subject
    TEXT = body

    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        server = smtplib.SMTP("mailhost.wsgc.com")
        server.ehlo()
        server.starttls()
#        server.login(user_email, user_pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print ('successfully sent the mail')
    except Exception as e:
        print ("failed to send mail: ", e)


def printUsageMessage():
  print ("Usage: killLongRunningImpalaQueries.py <queryRunningSeconds>  [KILL]")
  print ("Example that lists queries that have run more than 10 minutes:")
  print ("./killLongRunningImpalaQueries.py 600")
  print ("Example that kills queries that have run more than 10 minutes:")
  print ("./killLongRunningImpalaQueries.py 600 KILL")

## ** Validate command line args *************


if len(sys.argv) == 1 or len(sys.argv) > 3:
  printUsageMessage()
  quit(1)

queryRunningSeconds = sys.argv[1]

if not queryRunningSeconds.isdigit():
  print ("Error: the first argument must be a digit (number of seconds)")
  printUsageMessage()
  quit(1)

kill = False

if len(sys.argv) == 3:
  if sys.argv[2] != 'KILL':
    print ("the only valid second argument is \"KILL\"")
    printUsageMessage()
    quit(1)
  else:
    kill = True

impala_service = None

## Connect to CM
print ("\nConnecting to Cloudera Manager at " + cm_host + ":" + cm_port)
api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password, version=cm_api_version, use_tls=True)

## Get the Cluster
cluster = api.get_cluster(cluster_name)

## Get the IMPALA service
service_list = cluster.get_all_services()
#service_list = ['IMPALA']
for service in service_list:
  if service.type == "IMPALA":
    impala_service = service
    print ("Located Impala Service: " + service.name)
    break

if impala_service is None:
  print ("Error: Could not locate Impala Service")
  quit(1)

## A window of one day assumes queries have not been running more than 24 hours
now = datetime.utcnow()
start = now - timedelta(days=1)

print ("Looking for Impala queries running more than " + str(queryRunningSeconds) + " seconds")

if kill:
  print ("Queries will be killed")

filterStr = 'queryDuration > ' + queryRunningSeconds + 's'

impala_query_response = impala_service.get_impala_queries(start_time=start, end_time=now, filter_str=filterStr, limit=1000)
queries = impala_query_response.queries

longRunningQueryCount = 0

finalquerysend = ""
#finalquerylist = []
#finalquerydict = {}

for i in range (0, len(queries)):
  query = queries[i]

  if query.queryState != 'FINISHED' and query.queryState != 'EXCEPTION':

    longRunningQueryCount = longRunningQueryCount + 1

    if longRunningQueryCount == 1:
      print ('-- long running queries -------------')

    print ("queryState : " + query.queryState)
    print ("queryId: " + query.queryId)
    print ("user: " + query.user)
    print ("startTime: " + query.startTime.strftime(fmt))
    query_duration = now - query.startTime
    print ("query running time (seconds): " + str(query_duration.seconds + query_duration.days * 86400))
#    print ("SQL: " + query.statement)
    print ("\n")

    finalquery = "queryState : " + query.queryState + "".join('\n') + "queryId: " + query.queryId + "".join('\n') + "user: " + query.user + "".join('\n') + "startTime: " + query.startTime.strftime(fmt) + "".join('\n') + "query running time (seconds): " + str(query_duration.seconds + query_duration.days * 86400) + "".join('\n') + "".join('\n')

#    finalquerylist.append(finalquery)
#    finalquerydict[query.queryId] = finalquery
    finalquerysend = finalquerysend + "".join('\n') + finalquery

    if kill:
      print ("Attempting to kill query...")
      impala_service.cancel_impala_query(query.queryId)

    print ('-------------------------------------')

if longRunningQueryCount == 0:
  print ("No queries found")



if longRunningQueryCount > 0:
  send_email('wsi-cloudera-prod@wsgc.com', ['sumit@phdata.io'], 'WSI - PROD: Impala queries running from more than 1 hr', finalquerysend)

print ("done")
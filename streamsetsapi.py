import requests
import json
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

#os.system("export PYTHONHTTPSVERIFY=0")

dpm_auth_creds = {"userName":"user@cluster.io", "password":"******" }
headers = {"Content-Type": "application/json", "X-Requested-By": "SDC"}
auth_request  = requests.post("https://cloud.streamsets.com/security/public-rest/v1/authentication/login" , data=json.dumps(dpm_auth_creds), headers=headers)
cookies = auth_request.cookies
print(auth_request.status_code)
print(auth_request.headers)
# Need to pass value of SS-SSO-LOGIN cookie as X-SS-User-Auth-Token header
headers = {
  "Content-Type":"application/json",
  "X-Requested-By":"SCH",
  "X-SS-REST-CALL":"true",
  "X-SS-User-Auth-Token":auth_request.cookies['SS-SSO-LOGIN']
}
url = requests.get("https://node.cluster.io:18631/rest/v1/pipelines?orderBy=LAST_MODIFIED&order=ASC&label=system:allPipelines&offset=0&len=50&includeStatus=true", verify=False, headers=headers)
print(url.text)


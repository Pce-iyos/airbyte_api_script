
import requests
import json


def get_workspace_list(server,token=None):
    url = server

    #headers
    headers = {'Content-Type':'application/json'}

    workspace_id = str(input("Enter Workspace ID\n"))
    #body
    payload = {
        "workspaceId": workspace_id
    
    }

    #convert dict to json string
    resp = requests.post(url,headers= headers,data= json.dumps(payload,indent= 4))

    #validate response server for the header and body e.g status code
    assert resp.status_code == 200


    #print response full body as text

    print((resp.text))


def get_connection_list(server,token=None):
    url = server

    #headers
    headers = {'Content-Type':'application/json'}
    workspace_id = str(input("Enter Workspace ID\n"))
    #body
    payload = {
            "workspaceId": workspace_id
    
    }

    #convert dict to json string
    resp = requests.post(url,headers= headers,data= json.dumps(payload,indent= 15))

    #print response full body as text

    print((resp.text))



def trigger_manual_sync(server,token=None):
    url = server

    #headers
    headers = {'Content-Type':'application/json'}
    
    connection_id = str(input("Enter Connection ID\n"))
    #body
    payload = {
            "connectionId": connection_id
    
    }

    #convert dict to json string
    resp = requests.post(url,headers= headers,data= json.dumps(payload,indent= 4))

    #print response full body as text

    print((resp.text))



def get_log(server,token=None):
    url = server

    #headers
    headers = {'Content-Type':'application/json'}
    print("The allowed logtype include server log or scheduler log ")
    
    logtype = str(input("Enter logtype\n"))
    #body
    payload = {
            "logType": logtype
    
    }

    #convert dict to json string
    resp = requests.post(url,headers= headers,data= json.dumps(payload,indent= 4))

    #print response full body as text

    print((resp.text))



def get_recent_connection_job(server,token=None):
    url = server
    print("The allowed config type include: check_connection_source,check_connection_destination,discover_schema,get_spec,sync,reset_connection")
    #headers
    headers = {'Content-Type':'application/json'}
    
    configtype = str(input("Enter config type\n"))
    configI_id = str(input("Enter config id\n"))
    page_size= int(input("Enter page size"))
    row_offset= int(input("Enter row offset"))
    #body
    payload = {
        
        "configTypes": [
            configtype
            ],
        "configId": configI_id,
            "pagination": {
            "pageSize": page_size,
            "rowOffset": row_offset
        }
    
    }

    #convert dict to json string
    resp = requests.post(url,headers= headers,data= json.dumps(payload,indent= 4))

    #print response full body as text

    print((resp.text))


def get_job_info(server,token=None):
    url = server

    #headers
    headers = {'Content-Type':'application/json'}

    job_id = str(input("Enter Job ID\n"))
    #body
    payload = {
            "id": job_id
    
    }

    #convert dict to json string
    resp = requests.post(url,headers= headers,data= json.dumps(payload,indent= 4))




    #print response full body as text

    print((resp.text))



def airbyte_health_status(server):
    url = server

    #headers
    headers = {'Content-Type':'application/json'}

    
    resp_code = requests.get(url,headers= headers)
    print(resp_code)
    response_result= (json.dumps(resp_code.json(),indent= 4))

    #validate response server for the header and body e.g status code
    assert resp_code.status_code == 200


    #print response full body as text

    print(response_result)



def reset_connection_job(server,token=None):
    url = server

    #headers
    headers = {'Content-Type':'application/json'}
    
    connection_id = str(input("Enter Connection ID\n"))
    #body
    payload = {
            "connectionId": connection_id
    
    }

    #convert dict to json string
    resp = requests.post(url,headers= headers,data= json.dumps(payload,indent= 4))

    #print response full body as text

    print((resp.text))



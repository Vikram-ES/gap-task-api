import requests
import json
import pandas as pd 
import numpy as np
import paho.mqtt.client as paho
import string
import random
import time
import app_config.app_config as cfg
import os
import datetime
import pytz


config = cfg.getconfig()
PUBLIC_DATACENTER_URL = config["api"].get("public_datacenter_url", "NA")


topic_id = "60ae9143e284d016d3559dfb"
topic_line1 = "u/{}/GAP_GAP04.PLC04.MLD1_DATA_Anode_Geometric".format(topic_id)
topic_line2 = "u/{}/GAP_GAP03.PLC03.SCHENCK2_FEED_RATE".format(topic_id)
topic_line3 = "u/{}/GAP_GAP04.PLC04.MLD1_DATA_Anode_Number".format(topic_id)
topic_line4 = "u/{}/GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric".format(topic_id)
topic_line5 = "u/{}/GAP_GAP04.PLC04.MLD2_DATA_Anode_Number".format(topic_id)

port = 1883

client = paho.Client()

def on_log(client, userdata, obj, buff):
    print ("log:" + str(buff))


def on_connect(client, userdata, flags, rc):
    client.subscribe(topic_line1)
    client.subscribe(topic_line2)
    client.subscribe(topic_line3)
    client.subscribe(topic_line4)
    client.subscribe(topic_line5)
    print ("Connected!")


count1 = 0  
count2 = 0 
count3 = 0
count4 = 0
count5 = 0
unique_responses1 = []  
unique_responses2 = []  
unique_responses3 = []
unique_responses4 = []
unique_responses5 = []


unique_timestamps1 = set() 
unique_timestamps2 = set()  
unique_timestamps3 = set() 
unique_timestamps4 = set()
unique_timestamps5 = set()  

alertList = []
attachment_path = ''


flag1 = False
flag2 = False

result1 = 0
result2 = 0

n = 1 # no of alert in table


def uploadRefernceData(fileName):
    global attachment_path
    print(fileName)
    print(type(fileName))
    
    path = ""
    files = {'upload_file': open(str(path+fileName),'rb')}
   
    url=config['api']['meta']+"/attachments/tasks/upload"

    response = requests.post(url, files=files)
    print ("uploading")
    print (url)
    print ("+"*20)

    if(response.status_code==200):
        status ="success"
        data = response.content
        # Parse the JSON data
        parsed_data = json.loads(data)
        # Access the "name" from the parsed JSON data
        name = parsed_data['result']['files']['upload_file'][0]['name']
        attachment_path ="https://data.exactspace.co/exactapi/attachments/tasks/download/"+name

        
    else:
        status= (str(response.status_code) + str(response.content))
    print (response.status_code, response.content)

    return status

def task_attachment(alert_time):
    global attachment_path
    # Define the URL
    url = "http://127.0.0.1:1788/alertplot"

    # Define the data you want to send in the POST request (if needed)
    data = {
        "alertTime_start": alert_time,
    }

    # Set the headers to indicate that you are sending JSON data
    headers = {"Content-Type": "application/json"}

    # Make the POST request
    response = requests.post(url, json=data, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        print("getting response: ", response.content)
        fileName= response.content
        uploadRefernceData(fileName)
        
    else:
        print("Not getting any responese. response code: ", response.status_code)

    
    return


def calculation(input_df):
    try:
        # Copy the input DataFrame to avoid modifying the original data
        df = input_df.copy()
        df = df.dropna()

        # Filter the DataFrame based on conditions
        df = df[(df['SCHENCK2_FEED_RATE'] >= 5300) & (df['SCHENCK2_FEED_RATE'] < 6700) 
                & (df['Geo_density'] >= 1.56) & (df['Geo_density'] <= 1.69)
                ]

        df = df[df['anode_number'] % 1 == 0]
        df['anode_number'] = df['anode_number'].astype(int)
        #  Find consecutive duplicate values in the column and mark them for removal
        df['to_remove'] = df['anode_number'] == df['anode_number'].shift(1)

        df = df[df['to_remove'] != True]

        df['Geo_density'] = df['Geo_density'].round(3)

        # Reset the index to use the default integer index
        df.reset_index(drop=True, inplace=True)

        benchmark = 1.645

        df['z_scores'] = (df['Geo_density'] - benchmark) 

        
        negative_z_scores = df[df['z_scores'] < 0]
        print("smaple df :")
        print(df)
        print("negative df:")
        print(negative_z_scores)

       
        if not negative_z_scores.empty:
            result_size = len(negative_z_scores)
            sample_size = len(df)
            if result_size >= sample_size * 1 / 2:
                alert_time = negative_z_scores.iloc[0]['timestamp']
                return alert_time

        return 0
    except Exception as e:
        print("Error in calculation:", e)
        return 0

def process_responses():
    global count1, count2, count3, count4, count5, unique_responses1, unique_responses2, unique_timestamps1, unique_timestamps2, unique_timestamps3, unique_responses3, n, unique_responses5, unique_responses4, unique_timestamps4, unique_timestamps5
    global flag1, flag2, result1, result2, alertList
    print("...Geometric1 Density: {} Geometric2 Density: {} Schenk2: {} Anode number1: {} Anode number2: {}".format(count1,count4, count2, count3, count5))
    # Define the sample size
    sample_size = 50

    try:
        # Check if we have received enough packets for both topics
        if count1 >= sample_size and count2 >= sample_size and count3 >= sample_size:
            # Create DataFrames when sample size is met
            df1 = pd.DataFrame(unique_responses1)
            df2 = pd.DataFrame(unique_responses2)
            df3 = pd.DataFrame(unique_responses3)

            flag1 = True

            # Merge the DataFrames based on the 'timestamp' column
            merged_df = df1.merge(df2, on="timestamp", how="inner")
            merged_df = merged_df.merge(df3, on='timestamp', how="inner")
            
            print("merged df:")
            print(merged_df)

            # Calculate results based on merged data
            result1 = calculation(merged_df)
        
            # Reset counters and clear lists/sets
            count1 = 0
            count3 = 0
            n = 1  # alert Number
            unique_responses1[:] = []
            unique_responses3[:] = []

            unique_timestamps1 = set()
            unique_timestamps3 =set()
    
        if count2 >= sample_size and count4 >= sample_size and count5 >= sample_size:
                # Create DataFrames when sample size is met
                df6 = pd.DataFrame(unique_responses2)
                df4 = pd.DataFrame(unique_responses4)
                df5 = pd.DataFrame(unique_responses5)

                flag2 = True

                merged_df = df6.merge(df4, on="timestamp", how="inner")
                merged_df = merged_df.merge(df5, on='timestamp', how="inner")

                print("merged df:")
                print(merged_df)


                # # Calculate results based on merged data
                result2 = calculation(merged_df)

                # Reset counters and clear lists/sets
                count4 = 0
                count5 = 0
                n = 1  # alert Number
                unique_responses4[:] = []
                unique_responses5[:] = []

                unique_timestamps4 = set()
                unique_timestamps5 =set()

        if(result2 == 0 and result1 != 0):
                # Replace epoch_timestamp with your actual epoch timestamp
                epoch_timestamp = result1/1000  # Convert milliseconds to seconds

                # Specify the local timezone (e.g., 'US/Eastern', 'Europe/London', etc.)
                local_timezone = pytz.timezone('Asia/Kolkata')

                # Convert the epoch timestamp to a datetime object in the local timezone
                dt = datetime.datetime.fromtimestamp(epoch_timestamp, tz=local_timezone)

                # Format the datetime object as a string in the desired format
                formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                print('Email Sent to clint. (Demo)')

                task_attachment(epoch_timestamp)

                sendAlmEmail({"alert_time":formatted_date,"desc":"Geometric Density Going Low"})

                create_task(epoch_timestamp)

                result1 = 0
                result2 = 0
                count1 = 0
                count2 = 0
                count3 = 0
                count4 = 0 
                count5 = 0
                unique_responses1[:] = []
                unique_responses2[:] = []
                unique_responses3[:] = []
                unique_responses4[:] = []
                unique_responses5[:] = []

                unique_timestamps1 = set()
                unique_timestamps2 = set()
                unique_timestamps3 = set()
                unique_timestamps4 = set()
                unique_timestamps5 = set()
        elif(result2 != 0 and result1 == 0):
                # Replace epoch_timestamp with your actual epoch timestamp
                epoch_timestamp = result2/1000  # Convert milliseconds to seconds

                # Specify the local timezone (e.g., 'US/Eastern', 'Europe/London', etc.)
                local_timezone = pytz.timezone('Asia/Kolkata')

                # Convert the epoch timestamp to a datetime object in the local timezone
                dt = datetime.datetime.fromtimestamp(epoch_timestamp, tz=local_timezone)

                # Format the datetime object as a string in the desired format
                formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                print('Email Sent to clint. (Demo)')

                task_attachment(epoch_timestamp)

                sendAlmEmail({"alert_time":formatted_date,"desc":"Geometric Density Going Low"})

                create_task(epoch_timestamp)

                result1 = 0
                result2 = 0
                count1 = 0
                count2 = 0
                count3 = 0
                count4 = 0 
                count5 = 0
                unique_responses1[:] = []
                unique_responses2[:] = []
                unique_responses3[:] = []
                unique_responses4[:] = []
                unique_responses5[:] = []

                unique_timestamps1 = set()
                unique_timestamps2 = set()
                unique_timestamps3 = set()
                unique_timestamps4 = set()
                unique_timestamps5 = set()
        elif(result2 != 0 and result1 != 0):
               # Replace epoch_timestamp with your actual epoch timestamp
                epoch_timestamp = result2/1000  # Convert milliseconds to seconds

                # Specify the local timezone (e.g., 'US/Eastern', 'Europe/London', etc.)
                local_timezone = pytz.timezone('Asia/Kolkata')

                # Convert the epoch timestamp to a datetime object in the local timezone
                dt = datetime.datetime.fromtimestamp(epoch_timestamp, tz=local_timezone)

                # Format the datetime object as a string in the desired format
                formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")

                print('Email Sent to clint. (Demo)')

                task_attachment(epoch_timestamp)

                sendAlmEmail({"alert_time":formatted_date,"desc":"Geometric Density Going Low"})

                create_task(epoch_timestamp)
                result1 = 0
                result2 = 0
                count1 = 0
                count2 = 0
                count3 = 0
                count4 = 0 
                count5 = 0
                unique_responses1[:] = []
                unique_responses2[:] = []
                unique_responses3[:] = []
                unique_responses4[:] = []
                unique_responses5[:] = []

                unique_timestamps1 = set()
                unique_timestamps2 = set()
                unique_timestamps3 = set()
                unique_timestamps4 = set()
                unique_timestamps5 = set()

        if(flag1 and flag2):
            flag1 = False
            flag2 = False
            count2 = 0
            unique_responses2[:] = []
            unique_timestamps2 = set()

    except Exception as e:
        print("An error occurred in process_responses:", e)
       
def on_message(client, userdata, message):
    global count1, count2,count3,count4, count5, unique_timestamps1, unique_responses1, unique_timestamps2, unique_responses2, unique_timestamps3, unique_responses3, unique_responses4, unique_responses5, unique_timestamps4, unique_timestamps5  # Declare global variables

    incoming_msg = json.loads(message.payload)
    topic = message.topic
    # print(topic) # debug

    if topic == topic_line1:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            geo_density = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "Geo_density": geo_density,
                "timestamp":timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps1:
                unique_timestamps1.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses1.append(result)  # Add the response to the list of unique responses
                count1 += 1  # Increment the count variable

            process_responses()
        
    if topic == topic_line2:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            SCHENCK2_FEED_RATE = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "SCHENCK2_FEED_RATE": SCHENCK2_FEED_RATE,
                "timestamp": timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps2:
                unique_timestamps2.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses2.append(result)  # Add the response to the list of unique responses
                count2 += 1  # Increment the count variable
            process_responses()

    if topic == topic_line3:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            anode_number = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "anode_number": anode_number,
                "timestamp": timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps3:
                unique_timestamps3.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses3.append(result)  # Add the response to the list of unique responses
                count3 += 1  # Increment the count variable
            process_responses()

    if topic == topic_line4:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            geo_density = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "Geo_density": geo_density,
                "timestamp":timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps4:
                unique_timestamps4.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses4.append(result)  # Add the response to the list of unique responses
                count4 += 1  # Increment the count variable

            process_responses()

    if topic == topic_line5:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            anode_number = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "anode_number": anode_number,
                "timestamp": timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps5:
                unique_timestamps5.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses5.append(result)  # Add the response to the list of unique responses
                count5 += 1  # Increment the count variable
            process_responses()
    

def sendAlmEmail(alertArg):
    global alertList, n
    unitName, SiteName, CustomerName = 'GAP', 'Mahan', 'Hindalco'
    alertLevel = 'warning'
    

    if len(alertList) < 1:
        alertList.append(alertArg)
    else:
        alertList = []
        alertList.append(alertArg)

    emailTemplate = os.path.join(os.getcwd(), 'templates/almEmailTemplate.html')
    
    with open(emailTemplate, 'r') as f:
        s = f.read()

    if PUBLIC_DATACENTER_URL != 'NA':
        logoLink = 'img src="{}pulse-files/email-logos/logo.png"'.format(PUBLIC_DATACENTER_URL)
        s = s.replace('img src="#"', logoLink)
    else:
        logoLink = 'https://data.exactspace.co/pulse-files/email-logos/logo.png'
        s = s.replace('img src="#"', 'img src="{}"'.format(logoLink))

        
    if str(alertLevel) == "warning":
        s = s.replace("""<td colspan="3" align="left" style="border-bottom: solid 1px #CACACA; color:red; padding-bottom: 5px; font-size: 15px;"><b>Alarms Active(critical)</b></td>""",
                      """<td colspan="3" align="left" style="border-bottom: solid 1px #CACACA; color:#fd7e14; padding-bottom: 5px; font-size: 15px;"><b>Alarms Active(critical)</b></td>"""
                     )

    s = s.replace('UnitName', unitName)
    s = s.replace('SiteName', SiteName)
    s = s.replace('CustomerName', CustomerName)
    
    devTable = ''

    if alertList:
        devTable = '<tbody id="devList">'
        for alert in alertList:
            try:
                devTable += '''
                    <tr>
                        <td align="center" width="40" style="border-bottom: solid 1px #CACACA;">{}</td>
                        <td align="left" style="font-size: 13px; border-bottom: solid 1px #CACACA;">{}</td>
                        <td align="left" width="100" style="font-size: 13px; border-bottom: solid 1px #CACACA;">{}</td>
                    </tr>
                '''.format(n, alert.get('desc', ''), alert.get('alert_time', ''))
                n += 1
            except Exception as e:
                print('Error in creating a table of alert', e)
                return

        s = s.replace('<tbody id="devList">', devTable)

    else:
        print('Alarm with no open criticalTags, mail not sent!')
        return

    with open(os.path.join(os.getcwd(), 'templates/almEmailTemp.html'), 'wb') as f:
        f.write(s.encode('utf-8'))

    with open(os.path.join(os.getcwd(), 'templates/almEmailTemp.html'), 'r') as f:
        msg_body = f.read()

    try:
        url = config['api']['meta'].replace('exactapi', 'mail/send-mail')
        payload = json.dumps({
            'from': 'vikram.k@exactspace.co',
            'to': ['vikramkbgs@gmail.com'],
            'html': msg_body,
            'bcc': [],
            'subject': 'Testing Email Sending',
            'body': msg_body
        })
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=payload, headers=headers)

        if response.text == 'Success':
            print('Email sent to the client.')
            return 'Success'
        else:
            print('Error in sending mail', response.status_code)
            return 'Fail'

    except Exception as e:
        print('Error in sending mail', e)
        return 'Fail'

def create_task(alert_time):
    global attachment_path
    # Define the URL where you want to make the POST request
    url = "https://data.exactspace.co/exactapi/activities"

        # Replace epoch_timestamp with your actual epoch timestamp
    epoch_timestamp = alert_time

    # Specify the local timezone (e.g., 'US/Eastern', 'Europe/London', etc.)
    local_timezone = pytz.timezone('Asia/Kolkata')

    # Convert the epoch timestamp to a datetime object in the local timezone
    dt = datetime.datetime.fromtimestamp(epoch_timestamp, tz=local_timezone)

    # Add 2 hours to the datetime object
    dt_alert_due_time = dt + datetime.timedelta(hours=2)

    # Format the datetime object as a string in the desired format
    formatted_date_alert_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    formatted_date_alert_due_time = dt_alert_due_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    data = {
    "type": "task",
    "voteAcceptCount": 0,
    "voteRejectCount": 0,
    "acceptedUserList": [],
    "rejectedUserList": [],
    "dueDate": formatted_date_alert_due_time,
    "assignee": "5f491bb942ba5c3f7a474d15",
    "source": "Anode Forming",
    "team": "Operation",
    "createdBy": "5f491bb942ba5c3f7a474d15",
    "createdOn": formatted_date_alert_time,
    "siteId": "5cef6b03be741b86a8c893a0",
    "subTasks": [],
    "chats": [],
    "taskPriority": "--",
    "updateHistory": [],
    "unitsId": "60ae9143e284d016d3559dfb",
    "collaborators": [
        "632d3bd36d161904360db797"
    ],
    "status": "--",
    "content": [
        {
            "type": "title",
            "value": "GAP Density Going Low (Testing)"
        }
    ],
    "taskGeneratedBy": "system",
    "incidentId": "",
    "category": "",
    "sourceURL": "",
    "notifyEmailIds": [
        "vikram.k@exactspace.co"
    ],
    "chat": [],
    "taskDescription": "<p><img src=\"{}\"></p>".format(attachment_path),
    "triggerTimePeriod": "days",
    "viewedUsers": [],
    "completedBy": "",
    "equipmentIds": [],
    "mentions": [],
    "systems": []
}
    # Convert the data to a JSON string
    json_data = json.dumps(data)

    # Set the headers to indicate that you are sending JSON data
    headers = {"Content-Type": "application/json"}

    # Make the POST request
    response = requests.post(url, data=json_data, headers=headers)

    # Check the response
    if response.status_code == 200:
        print("Task Create request was successful")
        print("Response:", response.text)
    else:
        print("Task Create request failed with status code:", response.status_code)
        return

try:
    client.username_pw_set(username=config["BROKER_USERNAME"], password=config["BROKER_PASSWORD"])
except:
    pass
client.connect(config['BROKER_ADDRESS'], port)
# client.on_log = on_log
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()


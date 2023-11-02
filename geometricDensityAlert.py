###########################################################
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


config = cfg.getconfig()
PUBLIC_DATACENTER_URL = config["api"].get("public_datacenter_url", "NA")


topic_line1 = "u/60ae9143e284d016d3559dfb/GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric"
topic_line2 = "u/60ae9143e284d016d3559dfb/GAP_GAP03.PLC03.SCHENCK2_FEED_RATE"

port = 1883

client = paho.Client()

def on_log(client, userdata, obj, buff):
    print ("log:" + str(buff))


def on_connect(client, userdata, flags, rc):
    client.subscribe(topic_line1)
    client.subscribe(topic_line2)
    print ("Connected!")


count1 = 0  # Initialize the global count variable
count2 = 0 
unique_timestamps1 = set()  # Create a set to store unique timestamps
unique_responses1 = []  # Create a list to store unique responses based on timestamp
unique_timestamps2 = set()  # Create a set to store unique timestamps
unique_responses2 = []  # Create a list to store unique responses based on timestamp
alertList = []
n = 1

def calculation(input_df):
    try:
        # Copy the input DataFrame to avoid modifying the original data
        df = input_df.copy()

        # Filter the DataFrame based on conditions
        df = df[(df['SCHENCK2_FEED_RATE'] >= 5500) &
                (df['SCHENCK2_FEED_RATE'] < 6700) &
                (df['Geo_density'] >= 1.56) &
                (df['Geo_density'] <= 1.69)]

        # Convert and format the timestamp column
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Kolkata')
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Define the benchmark value
        benchmark = 1.645

        # Calculate the standard deviation of 'Geo_density'
        benchmark_std = 0
        benchmark_std = np.std(df['Geo_density'])

        print("Benchmark Standard Deviation:", benchmark_std)

        # Calculate 'z_scores' based on the benchmark and standard deviation
        if benchmark_std > 0:
            df['z_scores'] = (df['Geo_density'] - benchmark) / benchmark_std

        # Determine alert conditions and collect relevant data
        alert_data = {'flag': 'normal', 'alert_time': 0, 'desc': "Geometric Density above {:.2f}".format(benchmark)}

        negative_z_scores = df[df['z_scores'] < 0]

        if not negative_z_scores.empty:
            alert_time = negative_z_scores.iloc[0]['timestamp']
            alert_data['alert_time'] = alert_time

            result_size = len(negative_z_scores)
            sample_size = len(df)

            if result_size > sample_size * 2 / 3:
                alert_data['flag'] = 'warning'
                alert_data['desc'] = "Geometric Density less than {:.2f}".format(benchmark)
            elif result_size > sample_size / 2:
                alert_data['flag'] = 'critical'
                alert_data['desc'] = "Geometric Density less than {:.2f}".format(benchmark)

        return alert_data
    except Exception as e:
        print("Error in calculation:", e)
        return {'flag': 'error', 'alert_time': 0, 'desc': 'Error during calculation'}

def process_responses():
    global count1, count2, unique_responses1, unique_responses2, unique_timestamps1, unique_timestamps2
    # print("...Geometric Density Receiving Packet no. : {} Schenk2 Feed Rate Receiving Packet no. : {}".format(count1, count2))

    # Define the sample size
    sample_size = 100

    try:
        # Check if we have received enough packets for both topics
        if count1 >= sample_size and count2 >= sample_size:
            # Create DataFrames when sample size is met
            df1 = pd.DataFrame(unique_responses1)
            df2 = pd.DataFrame(unique_responses2)

            # Merge the DataFrames based on the 'timestamp' column
            merged_df = df1.merge(df2, on="timestamp", how="inner")

            # print("Inner Joined DataFrame:")
            # print(merged_df)

            # Calculate results based on merged data
            result = calculation(merged_df)

            flag = result.get('flag')
            alert_time = result.get('alert_time')

            if flag == 'warning' or flag == 'critical':
                sendAlmEmail(result)
            else:
                alertList = []

            # Reset counters and clear lists/sets
            count1 = count2 = 0
            unique_responses1[:] = []
            unique_responses2[:] = []
            unique_timestamps1 = set()
            unique_timestamps2 = set()

    except Exception as e:
        print("An error occurred in process_responses:", e)
       
def on_message(client, userdata, message):
    global count1, count2, unique_timestamps1, unique_responses1, unique_timestamps2, unique_responses2  # Declare global variables

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

def sendAlmEmail(alterArg):
    global alertList, n
    unitName, SiteName, CustomerName = 'GAP', 'Mahan', 'Hindalco'
    alertLevel = alterArg.get('desc', '')

    if len(alertList) < 5:
        alertList.append(alterArg)
    else:
        alertList = []

    emailTemplate = os.path.join(os.getcwd(), 'assets/email-template/almEmailTemplate.html')
    
    with open(emailTemplate, 'r') as f:
        s = f.read()

    if PUBLIC_DATACENTER_URL != 'NA':
        logoLink = 'img src="{}pulse-files/email-logos/logo.png"'.format(PUBLIC_DATACENTER_URL)
        s = s.replace('img src="#"', logoLink)
    else:
        logoLink = 'https://data.exactspace.co/pulse-files/email-logos/logo.png'
        s = s.replace('img src="#"', 'img src="{}"'.format(logoLink))

    if str(alertLevel) == "warning":
        s = s.replace("""<td colspan="3" align="left" style="border-bottom: solid 1px #CACACA; color:yellow; padding-bottom: 5px; font-size: 15px;"><b>Alarms Active(warning)</b></td>""",
                      """<td colspan="3" align="left" style="border-bottom: solid 1px #CACACA; color:#fd7e14; padding-bottom: 5px; font-size: 15px;"><b>Alarms Active(warning)</b></td>"""
                     )
        
    if str(alertLevel) == "critical":
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

    with open(os.path.join(os.getcwd(), 'assets/email-template/almEmailTemp.html'), 'wb') as f:
        f.write(s.encode('utf-8'))

    with open(os.path.join(os.getcwd(), 'assets/email-template/almEmailTemp.html'), 'r') as f:
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

   
try:
    client.username_pw_set(username=config["BROKER_USERNAME"], password=config["BROKER_PASSWORD"])
except:
    pass
client.connect(config['BROKER_ADDRESS'], port)
# client.on_log = on_log
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()


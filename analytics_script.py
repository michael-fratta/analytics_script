# python scheduler
import schedule
import time

def job():

    from dotenv import load_dotenv
    import os
    load_dotenv()

    # start counting elapsed time, in case of freeze
    now = time.perf_counter()

    ### assign keys for Airtable API
    from pyairtable import Table

    api_key = os.getenv('airtable_api_key')

    base_id = os.getenv('airtable_base_id')

    ### assign name to Table object

    table = Table(api_key, base_id, 'Airtable Table')

    ### create a list of airtable ref id, gaid, and email address ###
    ### ONLY IF it does not already have a Source, Medium, Channel Group, or Keyword ###
    # and only look at those rows added after the last script run #
    from datetime import date,timedelta,datetime

    print("Starting job ... \n")

    cl_fl = []
    for record in table.iterate():
        for each_record in record:
            if all(fields in each_record['fields'].keys() for fields in ['GA Client ID','Email']) and not \
               all(fields in each_record['fields'].keys() for fields in ['Source','Medium','Channel Group','Campaign','Keyword']) and \
                   datetime.strptime(str(each_record['fields']['Date'])[:10], '%Y-%m-%d').date() >= (date.today() - timedelta(days=1)):
                    cl_fl.append([each_record['id'],each_record['fields']['GA Client ID'],each_record['fields']['Email']])

    ### connect to google analytics ###

    import os
    from apiclient import discovery
    from google.oauth2 import service_account
    import json
    import socket

    socket.setdefaulttimeout(300) # 5 minutes; to avoid socket timeout error after default time of 1 minute

    scopes = ['https://www.googleapis.com/auth/analytics.readonly']

    secret_file = json.loads(os.getenv('secret'))

    credentials = service_account.Credentials.from_service_account_info(secret_file, scopes=scopes)

    service = discovery.build('analytics','v4',credentials=credentials,discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))

    ### get today's date ###

    from datetime import datetime

    today = datetime.today().date()

    ### UPDATE AIRTABLE ### AND ### GET Person ID FROM PD ###

    from apiclient import errors

    clients = [] # initialise empty list for appending data fetched from GA

    # initialise list to hold which airtable rows were updated
    airtable_updates = []

    # initialise list to hold GAIDs that do not exist in GA
    inexistent_gaids = []

    # initialise list to hold email addresses that don't exist in pd
    not_in_pd = []

    # initialise list to hold updated persons
    updated_persons = []

    # initialise list to hold failed updated persons
    failed_updated_persons = []

    for client in cl_fl: # for each client in cl_fl
        
        try: # search for the client in GA, based on GA ID
            response = service.userActivity().search(
                body={
                    "viewId": os.getenv('ga_view_id'),
                    "user": {
                        "type": "CLIENT_ID",
                        "userId": f"{client[1]}"
                    },
                    "dateRange": {
                        "startDate": "2021-11-01",
                        "endDate": f"{today}",
                    }
                }
            ).execute()
            
            # assign relevant data to variables
            source = response['sessions'][-1]['activities'][0]['source']
            medium = response['sessions'][-1]['activities'][0]['medium']
            channel_group = response['sessions'][-1]['activities'][0]['channelGrouping']
            campaign = response['sessions'][-1]['activities'][0]['campaign']
            keyword = response['sessions'][-1]['activities'][0]['keyword']
            
            ### AIRTABLE ###
            # update relevant row in Airtable, with relevant data from GA
            table.update(f'{client[0]}',
                        {
                            'Source': f'{source}',
                            'Medium': f'{medium}',
                            'Channel Group': f'{channel_group}',
                            'Campaign': f'{campaign}',
                            'Keyword': f'{keyword}'
                        })

            airtable_updates.append(client[0])
            
            ### PIPEDRIVE ###
            # create params to search for person in PD
            params = {
                'api_token': os.getenv('pipedrive_api_key'),
                'term': f'{client[2]}', # searchterm
                'fields': 'email', # searching email
                'exact_match': 'true' # only exact matches
            }

            import requests
            
            # get person data if exists
            getting_response = requests.get(f'https://your-domain.pipedrive.com/api/v1/persons/search', params=params)
            getting_data = getting_response.json()['data'] # convert to json
            
            if len(getting_data['items']): # if person exists in pipedrive
                person_id = getting_data['items'][0]['item']['id'] # get the person id
                
                # add 2nd part of gaid as int, ga id, email, person id, source, channel,campaign,keyword to clients list
                clients.extend([[int(client[1].split('.')[1]),client[1],client[2],person_id,source,channel_group,campaign,keyword]])  
                
            else: # if person does not exist
                not_in_pd.append(client[2])
            
        except errors.HttpError as err:
            # if GAID does not exist in GA (anymore)
            if err.resp.status == 400:
                inexistent_gaids.append(client[1])


        # see if too much time has passed that something went wrong
        later = time.perf_counter()
        # send a slack message if it's been an exhorbitant amount of time
        if later - now > 3600:
            slack_token = os.getenv('slack_password')
            slack_channel = '#script-alerts'
            # create func
            def post_message_to_slack(text):
                return requests.post('https://slack.com/api/chat.postMessage', {
                    'token': slack_token,
                    'channel': slack_channel,
                    'text': text,
                }).json()
            # create msg and post to slack
            slack_info = "pyscript-gap is taking ages! Need to check it out on Heroku - might just need a redeploy; sometimes Heroku stalls for long periods."
            post_message_to_slack(slack_info)

    ### remove duplicates from above clients list ###
    seen = set()
    clients_set = []
    for item in clients:
        t = tuple(item)
        if t not in seen:
            clients_set.append(item)
            seen.add(t)

    ### sort all by person id, then by 2nd int part of ga id ###
    from operator import itemgetter

    sorted_set = sorted(clients_set, key=itemgetter(3,0))

    ### turn list of lists to df ###

    import pandas as pd

    set_df = pd.DataFrame(sorted_set)

    ### create list of indeces of dupes to drop ###

    drop_idx = []
    for i in range(len(set_df)-1):
        if set_df.loc[i][3] == set_df.loc[i+1][3]:
            drop_idx.append(i+1)

    ### create new df to contain only uniques and drop the dupes, based on the drop_idx list created above ###

    dropped_set_df = set_df.drop(drop_idx)

    ### turn dropped_set_df back into list, for easy submit to PD ###

    dropped_set_list = dropped_set_df.values.tolist()

    # map keys

    client_id = "pipedrive_custom_field_key"
    source = "pipedrive_custom_field_key"
    channel = "pipedrive_custom_field_key"
    campaign = "pipedrive_custom_field_key"
    keyword = "pipedrive_custom_field_key"

    ### update Person in PD with relevant data ####

    token = {'api_token': os.getenv('pipedrive_api_key')}

    n = 0 # initialise counter

    for client in dropped_set_list:

        # assign person_id
        person_id = client[3]

        # get person
        get_person = requests.get(f'https://your-domain.pipedrive.com/api/v1/persons/{person_id}',params=token)

        # assign values to variables
        pd_client = get_person.json()['data'][client_id]
        pd_source = get_person.json()['data'][source]
        pd_channel = get_person.json()['data'][channel]
        pd_campaign = get_person.json()['data'][campaign]
        pd_keyword = get_person.json()['data'][keyword]

        # see if difference or None
        if pd_client == client[1]:
            put_client = None
        else:
            put_client = client[1]

        if pd_source == client[4]:
            put_source = None
        else:
            put_source = client[4]

        if pd_channel == client[5]:
            put_channel = None
        else:
            put_channel = client[5]

        if pd_campaign == client[6]:
            put_campaign = None
        else:
            put_campaign = client[6]

        if pd_keyword == client[7]:
            put_keyword = None
        else:
            put_keyword = client[7]

        # if all the same - then skip that Person
        if put_client == None and put_source == None and put_channel == None and put_campaign == None and put_keyword == None:
            pass
        else: #if there is a diff
            # create payload
            data = {
                client_id: put_client,
                source: put_source,
                channel: put_channel,
                campaign: put_campaign,
                keyword: put_keyword
            }
            # put to relevant Person
            response = requests.put(f'https://your-domain.pipedrive.com/api/v1/persons/{person_id}', params=token, data=data)
        
            if response.ok:
                updated_persons.append(person_id)
            else:
                failed_updated_persons.append(person_id)
                
        later = time.perf_counter()
        # send a slack message if it's been an exorbitant amount of time
        slack_token = os.getenv('slack_password')
        slack_channel = '#script-alerts'
        # create func
        def post_message_to_slack(text):
            return requests.post('https://slack.com/api/chat.postMessage', {
                'token': slack_token,
                'channel': slack_channel,
                'text': text,
            }).json()
        if later - now > 3600:
            # create msg and post to slack
            slack_info = "script is taking ages! Need to check it out on Heroku - might just need a redeploy."
            post_message_to_slack(slack_info)

    print("Job done!\n")

    if airtable_updates:
        if len(airtable_updates) == 1:
            print(f"The following row was updated in Airtable:",airtable_updates,"\n")
        else:
            print(f"The following {len(airtable_updates)} rows were updated in Airtable:",airtable_updates,"\n")

    if inexistent_gaids:
        print(f"The following {len(inexistent_gaids)} GAID/s do/es not exist in Google Analytics: ",inexistent_gaids,"\n")

    if not_in_pd:
        print(f"The following {len(not_in_pd)} email address/es (with GAID) from Airtable, do/es not exist in Pipedrive: ",not_in_pd,"\n")

    if updated_persons:
        if len(updated_persons) == 1:
            print(f"The following Person was updated in Pipedrive:",updated_persons,"\n")
        else:
            print(f"The following {len(updated_persons)} Persons were updated in Pipedrive:",updated_persons,"\n")

    if failed_updated_persons:
        print(f"The following {len(failed_updated_persons)} Person/s could not be updated in Pipedrive: ",failed_updated_persons,"\n")
        slack_info = f"Need to investigate why the following {len(failed_updated_persons)} Person/s could not be updated in Pipedrive: {failed_updated_persons}"

    ### end ###

# run script every dat at 2am (quietest time on website)
schedule.every().day.at("02:00").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)

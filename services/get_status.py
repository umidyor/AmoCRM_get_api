import json

from env import AMOCRM_SUBDOMAIN,ACCESS_TOKEN,API_URL
import os,requests,time,datetime
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()
from crm.models import Leads_status

import pandas as pd
def get_tokens():
    url = f"https://{AMOCRM_SUBDOMAIN}.amocrm.ru/oauth2/access_token"

    data = {
        "client_id": "b62d2bd2-0a6d-4313-98ab-8b1bc5d2a059",  # Replace with your actual client_id
        "client_secret": "uoLsHNPg7xlL0gSMQLEj4yBT9XmppWxSCC8kdqe8BqpYvHj5MgR33uXTArXiBIHY",  # Replace with your actual client_secret
        "grant_type": "authorization_code",
        "code": "def50200e5034c2c51f2b5f5611c13aed5cd8ff773b9d30b6ad2e72b8a80ed17a042b0297cb55e37c890543ee27edbe3d27b23af5878759f2c801875862474944519b03f01ab84641c59631420c5ad4e8701fac4bc212b8b7b7f2ee781c4dbcf65b32111d67d2e00293cc002ad1886c8b97a88894dc2640e98366c6151785c15170d3bbed0de2899e74448f50e8d88097d49c3c7139e4c0154ce663b2077b45eeb7dfd76ced96206b7f7bbac6a9087d68bddaae7935e5ecf81e039244b11262c5ff00160fce310c1ae3672b15b130b31911f0074b155f45ee2bb6e7faf8dba36c69d778b55e0209c42c91148d5481b012429e0567532ef3cbf5f31c6b3338792802297d9b4cd9a25c8af034bd6bd02b89e4110265e94ba48abd1ee4ffe1929052f79f0f8bd50f5e2ff8cc3715ea14e1feebb1c0ed7ce06f337fd85ff1d18e42d849f7f11a62064b41c46f14573043ab80bce9a13644179299ab90dee9e08f1965591c3e92e08201e07854256bbdb59efb7ce380e4f81132ae699fd792a8fe74ea551a21a040f588f89d9809deeed760cbf7e427a33a6ba66a30b09fa4bdf41fdcc05a13529bf227b01c08ba88edde15699c7b25f6615a31ce386f7b5ba5443acbbd8e5911e312ebfb357a80a90af685cf3f02f524ddd743855641274e159f8011732",  # Replace with the authorization code from the URL
        "redirect_uri": "https://2sonkhm.uz"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(url, headers=headers,data=data)

    if response.status_code == 200:
        tokens = response.json()
        with open("../access_token.txt", "w") as access_file:
            access_file.write(tokens['access_token'])

        with open("../refresh_token.txt", "w") as refresh_file:
            refresh_file.write(tokens['refresh_token'])
        print("Tokens obtained successfully:", tokens)
    else:
        print(f"Failed to obtain tokens: {response.status_code}, {response.text}")

def get_amo_data(endpoint, params=None):
    url = f'{API_URL}/{endpoint}'
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json',

    }
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 204:
            print("No content returned from AmoCRM API.")
            return None
        else:
            print(f"Failed to get data from AmoCRM: {response.status_code}, {response.text}")
            return None
    except requests.RequestException as e:
        print(f"An error occurred while making the API request: {e}")
        return None




import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

def get_all_leads(endpoint, params=None):
    url = f'{API_URL}/{endpoint}'
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    start_time = time.time()
    print(f"Start Time: {start_time}")

    # Define the keys to extract and their corresponding new names
    key_mapping = {
        "id": "lead_id",
        "name": "lead_name",
        "price": "price",
        "group_id": "group",
        "responsible_user_id": "responsible_user_id",
        "created_by": "created_by",
        "pipeline_id": "pipeline_id",
        "status_id": "status_id",
    }

    try:
        all_leads = []
        while url:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                leads = data.get("_embedded", {}).get("leads", [])
                if leads:
                    # Normalize data and filter required keys
                    leads_df = pd.json_normalize(leads)[list(key_mapping.keys())]
                    # Rename the columns
                    leads_df.rename(columns=key_mapping, inplace=True)
                    all_leads.append(leads_df)

                # Check if there is a next page
                url = data.get("_links", {}).get("next", {}).get("href")
                params = None
            else:
                print(f"Error fetching data: {response.status_code} - {response.text}")
                break
        if all_leads:
            final_df = pd.concat(all_leads, ignore_index=True)
            end_time = time.time()
            print(f"End Time: {end_time}")
            print(f"Total Time Taken: {end_time - start_time} seconds")
            return final_df
        else:
            print("No leads data found.")
            return pd.DataFrame(columns=key_mapping.values())
    except requests.RequestException as e:
        print(f"An error occurred while making the API request: {e}")
        return pd.DataFrame(columns=key_mapping.values())


class AmoCRM:
    def __init__(self, ):
        self.api_url = API_URL
        self.access_token = ACCESS_TOKEN
        self.last_sync_time = timezone.now() - timedelta(hours=1)  # Start with an initial sync time (1 hour ago)

    def get_amo_data(self, url, headers, params=None):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 204:
                print("No content returned from AmoCRM API.")
                return None
            else:
                print(f"Failed to get data from AmoCRM: {response.status_code}, {response.text}")
                return None
        except requests.RequestException as e:
            print(f"An error occurred while making the API request: {e}")
            return None

    def get_all_leads(self, endpoint, params=None):
        # Define the key mapping for column renaming
        key_mapping = {
            "id": "lead_id",
            "name": "lead_name",
            "price": "price",
            "group_id": "group",
            "responsible_user_id": "responsible_user_id",
            "created_by": "created_by",
            "pipeline_id": "pipeline_id",
            "status_id": "status_id",
        }

        # Set up headers for the requests
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
        }



        # Record the start time
        start_time = time.time()
        print(f"Start Time: {start_time}")

        # Initialize the list of leads to store the data
        all_leads = []

        # Base URL for the API
        url = f'{self.api_url}/{endpoint}'

        # Add last_time_sync filter to the params to only fetch updated leads since the last sync
        params = params or {}
        params.update({"last_time_sync__gte": self.last_sync_time.isoformat()})  # Filter based on last_sync_time

        # Fetch the first page of leads
        data = self.get_amo_data(url, headers, params=params)
        if not data:
            print("No data found.")


        # Process the initial page and get the next page URL
        leads = data.get("_embedded", {}).get("leads", [])
        if leads:
            # Normalize the leads data and apply the key mapping
            leads_df = pd.json_normalize(leads)[list(key_mapping.keys())]
            leads_df.rename(columns=key_mapping, inplace=True)
            leads_df = leads_df.astype({
                'responsible_user_id': 'object',
                'created_by': 'object',
                'pipeline_id': 'object',
                'status_id': 'object'
            })
            i=0
            # Fetch additional fields for each lead (responsible_user, created_by, pipeline, and status)
            for index, lead in leads_df.iterrows():
                responsible_user_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/users/{lead['responsible_user_id']}",headers)
                created_by_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/users/{lead['created_by']}", headers) if lead['created_by'] != 0 else {"name": "Unknown"}
                pipeline_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/leads/pipelines/{lead['pipeline_id']}", headers)
                status_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/leads/pipelines/{lead['pipeline_id']}/statuses/{lead['status_id']}", headers)

                # Update the DataFrame with the additional fetched data
                leads_df.at[index, 'responsible_user_id'] = responsible_user_data.get("name", "Unknown")
                leads_df.at[index, 'created_by'] = created_by_data.get("name", "Unknown")
                leads_df.at[index, 'pipeline_id'] = pipeline_data.get("name", "Unknown")
                leads_df.at[index, 'status_id'] = status_data.get("name", "Unknown")
                i+=1
                print(f"Add {i}: {lead['lead_id']}||{responsible_user_data.get('name', 'Unknown')}||{created_by_data.get('name', 'Unknown')}||{pipeline_data.get('name', 'Unknown')}||{status_data.get('name', 'Unknown')}")
            all_leads.append(leads_df)


        next_page_url = data.get("_links", {}).get("next", {}).get("href")
        print("Next page url(1):",next_page_url)
        all_urls = []
        while next_page_url:
            all_urls.append(next_page_url)

            data = self.get_amo_data(next_page_url, headers, params=params)
            next_page_url = data.get("_links", {}).get("next", {}).get("href")


        # Use ThreadPoolExecutor to fetch all the pages in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(self.get_amo_data, all_urls, [headers]*len(all_urls), [params]*len(all_urls)))
        number_leads=0
        for result in results:
            if result:
                leads = result.get("_embedded", {}).get("leads", [])
                if leads:
                    leads_df = pd.json_normalize(leads)[list(key_mapping.keys())]
                    leads_df.rename(columns=key_mapping, inplace=True)

                    # Fetch additional fields for each lead (responsible_user, created_by, pipeline, and status)
                    for index, lead in leads_df.iterrows():
                        responsible_user_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/users/{lead['responsible_user_id']}", headers)
                        created_by_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/users/{lead['created_by']}", headers) if lead['created_by'] != 0 else {"name": "Unknown"}
                        pipeline_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/leads/pipelines/{lead['pipeline_id']}", headers)
                        status_data = self.get_amo_data(f"https://pixeltechuz.amocrm.ru/api/v4/leads/pipelines/{lead['pipeline_id']}/statuses/{lead['status_id']}", headers)

                        # Update the DataFrame with the additional fetched data
                        leads_df.at[index, 'responsible_user'] = responsible_user_data.get("name", "Unknown")
                        leads_df.at[index, 'created_by'] = created_by_data.get("name", "Unknown")
                        leads_df.at[index, 'pipeline'] = pipeline_data.get("name", "Unknown")
                        leads_df.at[index, 'status'] = status_data.get("name", "Unknown")
                        number_leads+=1
                        print(f"Add {number_leads}: {lead['lead_id']}||{responsible_user_data.get('name', 'Unknown')}||{created_by_data.get('name', 'Unknown')}||{pipeline_data.get('name', 'Unknown')}||{status_data.get('name', 'Unknown')}")

                    all_leads.append(leads_df)

        # Combine all data into a single DataFrame
        if all_leads:
            final_df = pd.concat(all_leads, ignore_index=True)
            self.last_sync_time = timezone.now()  # Update last sync time after successful sync



            # Record the end time
            end_time = time.time()
            print(f"End Time: {end_time}")
            print(f"Total Time Taken: {end_time - start_time} seconds")
            return final_df
        else:
            print("No leads data found.")




# class AmoCRM:
#     def __init__(self):
#         self.api_url =API_URL
#         self.access_token = ACCESS_TOKEN
#
#     def get_amo_data(self, endpoint, params=None):
#         """
#         Fetches data from AmoCRM API for a given endpoint.
#
#         Args:
#         - endpoint (str): The API endpoint to fetch data from.
#         - params (dict, optional): Optional query parameters for the API request.
#
#         Returns:
#         - dict or None: Parsed JSON data from the API or None if an error occurs.
#         """
#         url = f'{self.api_url}/{endpoint}'
#         headers = {
#             'Authorization': f'Bearer {self.access_token}',
#             'Content-Type': 'application/json',
#         }
#
#         try:
#             response = requests.get(url, headers=headers, params=params)
#             if response.status_code == 200:
#                 return response.json()
#             elif response.status_code == 204:
#                 print("No content returned from AmoCRM API.")
#                 return None
#             else:
#                 print(f"Failed to get data from AmoCRM: {response.status_code}, {response.text}")
#                 return None
#         except requests.RequestException as e:
#             print(f"An error occurred while making the API request: {e}")
#             return None
#
#     def get_all_leads(self,endpoint, params=None):
#         url = f'{API_URL}/{endpoint}'
#         headers = {
#             'Authorization': f'Bearer {ACCESS_TOKEN}',
#             'Content-Type': 'application/json',
#         }
#
#         # Define the keys to extract and their corresponding new names
#         key_mapping = {
#             "id": "lead_id",
#             "name": "lead_name",
#             "price": "price",
#             "group_id": "group",
#             "responsible_user_id": "responsible_user_id",
#             "created_by": "created_by",
#             "pipeline_id": "pipeline_id",
#             "status_id": "status_id",
#         }
#
#         try:
#             all_leads = []
#             while url:
#                 response = requests.get(url, headers=headers, params=params)
#                 if response.status_code == 200:
#                     data = response.json()
#                     leads = data.get("_embedded", {}).get("leads", [])
#                     if leads:
#                         # Normalize data and filter required keys
#                         leads_df = pd.json_normalize(leads)[list(key_mapping.keys())]
#                         # Rename the columns
#                         leads_df.rename(columns=key_mapping, inplace=True)
#                         all_leads.append(leads_df)
#
#                     # Check if there is a next page
#                     url = data.get("_links", {}).get("next", {}).get("href")
#                     params = None
#                 else:
#                     print(f"Error fetching data: {response.status_code} - {response.text}")
#                     break
#             if all_leads:
#                 final_df = pd.concat(all_leads, ignore_index=True)
#                 return final_df
#             else:
#                 print("No leads data found.")
#                 return pd.DataFrame(columns=key_mapping.values())
#         except requests.RequestException as e:
#             print(f"An error occurred while making the API request: {e}")
#             return pd.DataFrame(columns=key_mapping.values())


from django.db.models import Q
def get_all():
    while True:
        leads = get_amo_data("leads")["_embedded"]["leads"]
        for lead in leads:
            lead_id = lead.get('id')
            lead_name = lead.get('name')
            price = lead.get('price')
            group = lead.get('group_id')
            responsible_user = get_amo_data(f"users/{lead.get('responsible_user_id')}")["name"]
            created_by=get_amo_data(f"users/{lead.get('created_by')}")["name"] if lead.get('created_by')!=0 else "Unknown"
            pipeline = get_amo_data(f"leads/pipelines/{lead.get('pipeline_id')}")["name"]
            status_data = get_amo_data(f"leads/pipelines/{lead.get('pipeline_id')}/statuses/{lead.get('status_id')}")
            status = status_data.get('name', 'Unknown')

            data = {
                'lead_name': lead_name,
                'group': group,
                'price':price,
                'responsible_user': responsible_user,
                'created_by':created_by,
                'pipeline': pipeline,
                'status': status,
            }

            # Check if the lead already exists
            try:
                existing_lead = Leads_status.objects.get(lead_id=lead_id)
                print(f"Lead exists: {lead_name} (ID: {lead_id})")
                # Check if any field is different and update only if needed
                updated = False
                for field, value in data.items():
                    if getattr(existing_lead, field) != value:
                        print(f"Field '{field}' changed: Old value='{getattr(existing_lead, field)}', New value='{value}'")
                        setattr(existing_lead, field, value)
                        updated = True

                if updated:
                    existing_lead.save()
                    print(f"Updated lead: {lead_name} (ID: {lead_id})")
                else:
                    print(f"No changes for lead: {lead_name} (ID: {lead_id})")
            except Leads_status.DoesNotExist:
                # Create a new lead if it doesn't exist
                Leads_status.objects.create(
                    lead_id=lead_id,
                    **data
                )
                print(f"Added new lead: {lead_name} (ID: {lead_id})")

def get_all1():
    last_sync_time = datetime.datetime.now() - datetime.timedelta(days=30)

    while True:
        # Fetch updated leads since the last sync time
        params = {"filter[updated_at][from]": int(last_sync_time.timestamp())}
        amo_data = get_amo_data("leads", params=params)
        print(amo_data)
        if not amo_data:  # If the API call fails, retry after some time
            print("Failed to retrieve leads. Retrying in 5 seconds...")
            time.sleep(5)
            continue

        leads = amo_data.get("_embedded", {}).get("leads", [])

        if not leads:
            print("No new or updated leads found.")
            print("Waiting 5 seconds before next sync...")
            time.sleep(5)
            continue

        # Update the last sync time to now, for the next iteration
        last_sync_time = datetime.datetime.now()
        inter=0
        for lead in leads:
            inter+=1
            lead_id = lead.get('id')
            lead_name = lead.get('name')
            price = lead.get('price')
            group = lead.get('group_id')

            # Fetch related data from API
            responsible_user = get_amo_data(f"users/{lead.get('responsible_user_id')}").get("name", "Unknown")
            created_by = get_amo_data(f"users/{lead.get('created_by')}").get("name", "Unknown") if lead.get(
                'created_by') != 0 else "Unknown"
            pipeline = get_amo_data(f"leads/pipelines/{lead.get('pipeline_id')}").get("name", "Unknown")
            status_data = get_amo_data(f"leads/pipelines/{lead.get('pipeline_id')}/statuses/{lead.get('status_id')}")
            status = status_data.get('name', 'Unknown')

            # Prepare data for database update or creation
            data = {
                'lead_name': lead_name,
                'group': group,
                'price': price,
                'responsible_user': responsible_user,
                'created_by': created_by,
                'pipeline': pipeline,
                'status': status,
            }

            try:
                # Try to fetch the existing lead from the database
                existing_lead = Leads_status.objects.get(lead_id=lead_id)
                print(f"Lead exists: {lead_name} (ID: {lead_id}) Number:{inter}")

                # Check if any field is different and update only if needed
                updated = False
                for field, value in data.items():
                    if getattr(existing_lead, field) != value:
                        print(
                            f"Field '{field}' changed: Old value='{getattr(existing_lead, field)}', New value='{value}'")
                        setattr(existing_lead, field, value)
                        updated = True

                if updated:
                    existing_lead.save()
                    print(f"Updated lead: {lead_name} (ID: {lead_id})")
                else:
                    print(f"No changes for lead: {lead_name} (ID: {lead_id})")

            except Leads_status.DoesNotExist:
                # If the lead doesn't exist, create a new one
                Leads_status.objects.create(
                    lead_id=lead_id,
                    **data
                )
                print(f"Added new lead: {lead_name} (ID: {lead_id})")

        # Wait before the next sync cycle
        print("Waiting 5 seconds before next sync...")
        time.sleep(5)


# def process_leads_with_pd(leads):
#     # Normalize the leads data into a DataFrame
#     leads_df = pd.json_normalize(leads)
#
#     # Rename or select specific columns if needed
#     leads_df = leads_df.rename(columns={
#         "id": "lead_id",
#         "name": "lead_name",
#         "price": "price",
#         "group_id": "group",
#         "responsible_user_id": "responsible_user_id",
#         "created_by": "created_by",
#         "pipeline_id": "pipeline_id",
#         "status_id": "status_id"
#     })
#
#     # Extract additional information by making further API calls
#     leads_df["responsible_user"] = leads_df["responsible_user_id"].apply(
#         lambda user_id: get_amo_data(f"users/{user_id}").get("name", "Unknown") if user_id else "Unknown"
#     )
#     leads_df["created_by_name"] = leads_df["created_by"].apply(
#         lambda creator_id: get_amo_data(f"users/{creator_id}").get("name", "Unknown") if creator_id != 0 else "Unknown"
#     )
#     leads_df["pipeline"] = leads_df["pipeline_id"].apply(
#         lambda pipeline_id: get_amo_data(f"leads/pipelines/{pipeline_id}").get("name", "Unknown") if pipeline_id else "Unknown"
#     )
#     leads_df["status"] = leads_df.apply(
#         lambda row: get_amo_data(f"leads/pipelines/{row['pipeline_id']}/statuses/{row['status_id']}").get("name", "Unknown")
#         if row["pipeline_id"] and row["status_id"] else "Unknown", axis=1
#     )
#
#     return leads_df


# def get_all_with_pd():
#     last_sync_time = datetime.datetime.now() - datetime.timedelta(days=1)
#     all_leads_data = []
#
#     while True:
#         params = {"filter[updated_at][from]": int(last_sync_time.timestamp())}
#         amo_data = get_amo_data("leads", params=params)
#
#         if not amo_data:
#             print("Failed to retrieve leads. Retrying in 5 seconds...")
#             time.sleep(5)
#             continue
#
#         leads = amo_data.get("_embedded", {}).get("leads", [])
#         if not leads:
#             print("No new or updated leads found. Waiting 5 seconds before next sync...")
#             time.sleep(5)
#             continue
#
#         # Process leads using pd.json_normalize
#         leads_df = process_leads_with_pd(leads)
#
#         # Iterate through the DataFrame rows and save/update data in the database
#         for _, lead in leads_df.iterrows():
#             lead_data = lead.to_dict()
#             lead_id = lead_data.pop("lead_id")
#
#             try:
#                 existing_lead = Leads_status.objects.get(lead_id=lead_id)
#                 updated = False
#                 for field, value in lead_data.items():
#                     if getattr(existing_lead, field) != value:
#                         setattr(existing_lead, field, value)
#                         updated = True
#
#                 if updated:
#                     existing_lead.save()
#                     print(f"Updated lead: {lead['lead_name']} (ID: {lead_id})")
#             except Leads_status.DoesNotExist:
#                 Leads_status.objects.create(lead_id=lead_id, **lead_data)
#                 print(f"Added new lead: {lead['lead_name']} (ID: {lead_id})")
#
#         last_sync_time = datetime.datetime.now()
#         print("Waiting 5 seconds before next sync...")
#         time.sleep(5)


from django.utils import timezone
from datetime import timedelta

# class Save_model:
#     def __init__(self, data):
#         self.data = data  # Data should be a DataFrame
#
#     def tomodel(self):
#
#         # Prepare lists for bulk operations
#         leads_to_create = []
#         leads_to_update = []
#         start_time = time.time()
#         print(f"Start Time: {start_time}")
#
#         # Define the time period for sync (e.g., 1 hour ago)
#         last_sync_time = timezone.now() - timedelta(hours=1)  # Change the time window as needed
#
#         # Fetch existing lead_ids from the database that were modified since last sync
#         existing_leads = Leads_status.objects.filter(
#             lead_id__in=self.data['lead_id'],
#         ).values('lead_id', 'status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline', 'last_time_sync')
#
#
#
#         existing_leads_map = {lead['lead_id']: lead for lead in existing_leads}
#         ids = [h for h in existing_leads_map.keys()]
#
#         for _, row in self.data.iterrows():
#             lead_id = row['lead_id']
#
#             if str(lead_id) in ids:
#
#                 existing_lead = existing_leads_map[str(lead_id)]
#                 if (
#                         existing_lead['status'] != row['status_id']
#                         or existing_lead['price'] != row.get('price', None)
#                         or existing_lead['lead_name'] != row['lead_name']
#                         or existing_lead['responsible_user'] != row['responsible_user_id']
#                         or existing_lead['created_by'] != row['created_by']
#                         or existing_lead['group'] != row['group']
#                         or existing_lead['pipeline'] != row['pipeline_id']
#                 ):
#                     # Update existing lead object
#                     lead_obj = Leads_status.objects.get(lead_id=existing_lead['lead_id'])
#                     lead_obj.lead_name = row['lead_name']
#                     lead_obj.responsible_user = row['responsible_user_id']
#                     lead_obj.created_by = row['created_by']
#                     lead_obj.group = row['group']
#                     lead_obj.price = row.get('price', None)
#                     lead_obj.pipeline = row['pipeline_id']
#                     lead_obj.status = row['status_id']
#                     lead_obj.last_time_sync = timezone.now()
#                     leads_to_update.append(lead_obj)
#
#             else:
#                 # If lead_id doesn't exist in the database, prepare for creation
#                 leads_to_create.append(
#                     Leads_status(
#                         lead_id=lead_id,
#                         lead_name=row['lead_name'],
#                         responsible_user=row['responsible_user_id'],
#                         created_by=row['created_by'],
#                         group=row['group'],
#                         price=row.get('price', None),
#                         pipeline=row['pipeline_id'],
#                         status=row['status_id'],
#                         last_time_sync=timezone.now(),  # Set current time as sync time
#                     )
#                 )
#                 print(f"Add {row['lead_id']}")
#
#         # Perform bulk operations
#         if leads_to_create:
#             Leads_status.objects.bulk_create(leads_to_create, batch_size=1000)
#         if leads_to_update:
#             Leads_status.objects.bulk_update(
#                 leads_to_update,
#                 fields=['status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline', 'last_time_sync'],
#                 batch_size=1000
#             )
#
#
#         end_time = time.time()
#         print(f"End Time: {end_time}")
#         print(f"Total Time Taken: {end_time - start_time} seconds")
#
#         print(f"{len(leads_to_create)} leads created, {len(leads_to_update)} leads updated!")
import logging

# Set up logger
logger = logging.getLogger(__name__)

class Save_model:
    def __init__(self, data):
        self.data = data  # Data should be a DataFrame

    def tomodel(self):
        # Prepare lists for bulk operations
        leads_to_create = []
        leads_to_update = []
        start_time = time.time()
        logger.info(f"Start Time: {start_time}")

        # Define the time period for sync (e.g., 1 hour ago)
        last_sync_time = timezone.now() - timedelta(hours=1)  # Change the time window as needed

        try:
            # Fetch existing lead_ids from the database that were modified since last sync
            existing_leads = Leads_status.objects.filter(
                lead_id__in=self.data['lead_id'],
            ).values('lead_id', 'status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline', 'last_time_sync')

            existing_leads_map = {lead['lead_id']: lead for lead in existing_leads}
            ids = [h for h in existing_leads_map.keys()]

            for _, row in self.data.iterrows():
                lead_id = row['lead_id']

                if str(lead_id) in ids:
                    existing_lead = existing_leads_map[str(lead_id)]
                    if (
                            existing_lead['status'] != row['status_id']
                            or existing_lead['price'] != row.get('price', None)
                            or existing_lead['lead_name'] != row['lead_name']
                            or existing_lead['responsible_user'] != row['responsible_user_id']
                            or existing_lead['created_by'] != row['created_by']
                            or existing_lead['group'] != row['group']
                            or existing_lead['pipeline'] != row['pipeline_id']
                    ):
                        # Update existing lead object
                        lead_obj = Leads_status.objects.get(lead_id=existing_lead['lead_id'])
                        lead_obj.lead_name = row['lead_name']
                        lead_obj.responsible_user = row['responsible_user_id']
                        lead_obj.created_by = row['created_by']
                        lead_obj.group = row['group']
                        lead_obj.price = row.get('price', None)
                        lead_obj.pipeline = row['pipeline_id']
                        lead_obj.status = row['status_id']
                        lead_obj.last_time_sync = timezone.now()
                        leads_to_update.append(lead_obj)

                else:
                    # If lead_id doesn't exist in the database, prepare for creation
                    leads_to_create.append(
                        Leads_status(
                            lead_id=lead_id,
                            lead_name=row['lead_name'],
                            responsible_user=row['responsible_user_id'],
                            created_by=row['created_by'],
                            group=row['group'],
                            price=row.get('price', None),
                            pipeline=row['pipeline_id'],
                            status=row['status_id'],
                            last_time_sync=timezone.now(),  # Set current time as sync time
                        )
                    )
                    logger.info(f"Add {row['lead_id']}")

            # Perform bulk operations using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = []
                if leads_to_create:
                    futures.append(executor.submit(self.bulk_create_leads, leads_to_create))
                if leads_to_update:
                    futures.append(executor.submit(self.bulk_update_leads, leads_to_update))

                # Wait for all tasks to complete
                for future in futures:
                    future.result()  # This will raise any exception that occurred during the execution

        except Exception as e:
            logger.error(f"Error during lead processing: {str(e)}")
        finally:
            end_time = time.time()
            logger.info(f"End Time: {end_time}")
            logger.info(f"Total Time Taken: {end_time - start_time} seconds")
            logger.info(f"{len(leads_to_create)} leads created, {len(leads_to_update)} leads updated!")

    def bulk_create_leads(self, leads_to_create):
        """Bulk create leads."""
        try:
            if leads_to_create:
                Leads_status.objects.bulk_create(leads_to_create, batch_size=1000)
                logger.info(f"Bulk created {len(leads_to_create)} leads.")
        except Exception as e:
            logger.error(f"Error during bulk_create: {str(e)}")

    def bulk_update_leads(self, leads_to_update):
        """Bulk update leads."""
        try:
            if leads_to_update:
                Leads_status.objects.bulk_update(
                    leads_to_update,
                    fields=['status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline', 'last_time_sync'],
                    batch_size=1000
                )
                logger.info(f"Bulk updated {len(leads_to_update)} leads.")
        except Exception as e:
            logger.error(f"Error during bulk_update: {str(e)}")

# if __name__ == "__main__":
    # get_tokens()
    # while True:
    #     data = AmoCRM()
    #     data=data.get_all_leads(endpoint="leads?page=1")
    #     print(data)
    #     savemodel = Save_model(data=data)
    #     result = savemodel.tomodel()
    #     print(result)
    #     time.sleep(30)
    #     continue


import requests

API_URL = "https://pbx23859.onlinepbx.ru/ivr"
API_KEY = "VDltUEpQS1g4Szg4a3pkelAzTVliWTlraDlMbXF6ZkY"

# Headers for authentication
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# Parameters (example: fetch calls from a date range)
params = {
    "start_date": "2024-12-19T00:00:00",
    "end_date": "2024-12-26T23:59:59",  # Fixed typo
}

# Make the GET request
response = requests.get(API_URL, headers=headers,)

# Check response status and print data
if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code} - {response.text}")


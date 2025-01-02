import asyncio
import json

import aiohttp
import time
from datetime import datetime
import pandas as pd
from env import *
import logging
from bot import *
import django,os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()
from crm.models import *
from django.db import transaction
logger = logging.getLogger(__name__)



class LeadProcessor:
    def __init__(self):
        self.access_token=ACCESS_TOKEN
        self.user_cache = {}
        self.pipeline_cache = {}
        self.status_cache = {}


    async def get_tokens(self):
        url = f"https://{AMOCRM_SUBDOMAIN}.amocrm.ru/oauth2/access_token"
        try:
            with open("tokens_file.json", mode="r") as json_file:
                tokens_data = json.load(json_file)

            data = {
                "client_id": tokens_data.get("client_id"),
                "client_secret": tokens_data.get("client_secret"),
                "grant_type": "authorization_code",
                "code": tokens_data.get("code"),
                "redirect_uri": tokens_data.get("redirect_uri"),
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            response = requests.post(url, headers=headers, data=data)

            if response.status_code == 200:
                tokens = response.json()
                with open("access_token.txt", "w") as access_file:
                    access_file.write(tokens['access_token'])

                with open("refresh_token.txt", "w") as refresh_file:
                    refresh_file.write(tokens['refresh_token'])
                print("Tokens obtained successfully:", tokens)
            else:
                print(f"Failed to obtain tokens: {response.status_code}, {response.text}")
        except json.JSONDecodeError:
            print("Error: Could not read tokens_file.json. Ensure it has valid content.")
        except Exception as e:
            print(f"Error occurred in get_tokens: {e}")

    async def refresh_token(self):
        url = f"https://{AMOCRM_SUBDOMAIN}.amocrm.ru/oauth2/access_token"

        try:
            with open("tokens_file.json", "r") as json_file:
                tokens_data = json.load(json_file)

            with open("refresh_token.txt", "r") as refresh_file:
                refresh_token = refresh_file.read().strip()

            data = {
                "client_id": tokens_data.get("client_id"),
                "client_secret": tokens_data.get("client_secret"),
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "redirect_uri": tokens_data.get("redirect_uri"),
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        tokens = await response.json()
                        self.access_token = tokens['access_token']

                        with open("access_token.txt", "w") as access_file:
                            access_file.write(self.access_token)

                        with open("refresh_token.txt", "w") as refresh_file:
                            refresh_file.write(tokens['refresh_token'])

                        print("Access token refreshed successfully.")
                        await problems("Access token refreshed successfully.")
                        return True
                    else:
                        print(f"Failed to refresh token: {response.status}, {await response.text()}")
                        await problems(f"Failed to refresh token: {response.status}, {await response.text()}")
                        return False


        except Exception as e:
            print(f"An error occurred while refreshing the token: {e}")
            return False
        finally:
            await session.close()

    async def fetch(self, session, url):
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
        }

        try:
            async with session.get(url, headers=headers) as response:
                remaining_requests = int(response.headers.get('X-RateLimit-Remaining', 0))
                reset_time = int(response.headers.get('X-RateLimit-Reset', time.time()))

                if response.status == 200:
                    return await response.json()

                if response.status == 429:  # Rate limit exceeded

                    wait_time = max(0, reset_time - time.time())

                    await asyncio.sleep(wait_time)

                    return await self.fetch(session, url)

                if response.status == 401:  # Unauthorized
                    await problems("Received 401 Unauthorized. Attempting to refresh token...")
                    print("Received 401 Unauthorized. Attempting to refresh token...")
                    if await self.refresh_token():
                        headers['Authorization'] = f'Bearer {self.access_token}'
                        return await self.fetch(session, url)
                    else:
                        print("Failed to refresh token.")
                        return None

        except Exception as e:
            print(f"An error occurred while making the API request: {e}")
            return None

    async def get_user(self, user_id):
        if user_id in self.user_cache:
            return self.user_cache[user_id]
        with open("JSONS/users.json", "r") as f:
            data=json.load(f)
            for element in data:
                for user in element['_embedded']['users']:
                    if user['id'] == user_id:
                        self.user_cache[user_id] = user['id']
                        return user['id']
        return "Unknown"

    async def get_pipeline(self, pipeline_id):
        if pipeline_id in self.pipeline_cache:
            return self.pipeline_cache[pipeline_id]

        # Fetch and cache the pipeline if not in cache
        with open("JSONS/pipelines.json", "r") as file:
            data = json.load(file)
            for element in data:
                for pipeline in element['_embedded']['pipelines']:
                    if pipeline['id'] == pipeline_id:
                        self.pipeline_cache[pipeline_id] = pipeline['id']  # Cache the pipeline name
                        return pipeline['id']
        return "Unknown"

    async def get_status(self, pipeline_id, status_id):
        if (pipeline_id, status_id) in self.status_cache:
            return self.status_cache[(pipeline_id, status_id)]

        # Fetch and cache the status if not in cache
        with open("JSONS/pipelines.json", "r") as file:
            data = json.load(file)
            for element in data:
                for pipeline in element['_embedded']['pipelines']:
                    if pipeline['id'] == pipeline_id:
                        for status in pipeline['_embedded']['statuses']:
                            # Check if the status_id matches, then return the name
                            if status['id'] == status_id:
                                self.status_cache[(pipeline_id, status_id)] = status['id']
                                return status['id']

        # Return a default message or status_id if not found
        return "Unknown"

        # async def fetch_all_and_save(self, filename, endpoint):
    #     pages = []  # List to store all the page URLs
    #     page = 1  # Start with the first page
    #     first_page = True  # Flag to handle the first write in the JSON file
    #     start_time = time.time()
    #     print(f"Start Time for {endpoint}: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    #     # Open the JSON file to write responses directly to it
    #     with open(f"JSONS/{filename}.json", "w") as file:
    #         # Start the session
    #         async with aiohttp.ClientSession() as session:
    #
    #             while True:
    #                 url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page={page}"
    #                 response = await self.fetch(session, url)
    #                 if response and "next" in response.get("_links", {}):
    #                     pages.append(url)
    #                     page += 1
    #                 else:
    #                     pages.append(url)
    #                     break
    #
    #             # Use asyncio.gather to fetch all pages concurrently
    #             responses = await asyncio.gather(*(self.fetch(session, url) for url in pages))
    #
    #             # Write the response data directly to the JSON file
    #             if first_page:
    #                 json.dump(responses, file, indent=4)
    #                 first_page = False
    #             else:
    #                 file.seek(file.tell() - 1)  # Go back to overwrite the last comma
    #                 json.dump(responses, file, indent=4)
    #                 file.write(',\n')  # Add a comma after each entry
    #
    #             # Write the closing ']' after the last page's data
    #             file.write('\n')
    #             print(f"Data saved to {filename}.json")
    #             end_time = time.time()
    #             print(
    #                 f"End Time for {endpoint}: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')} - Total Time Taken: {end_time - start_time:.2f} seconds")
    #             print(pages)

    async def fetch_all_and_save(self,filename, endpoint):
        start_time = time.time()
        print(f"Start Time for {endpoint}: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")

        with open(f"JSONS/{filename}.json", "w") as file:
            async with aiohttp.ClientSession() as session:
                # Step 1: Fetch the first page
                first_page_url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page=1"
                first_response = await self.fetch(session, first_page_url)

                if not first_response:
                    print(f"Failed to fetch the first page for {endpoint}.")
                    return

                # Determine if pagination is present
                total_pages = 1
                if "next" in first_response.get("_links", {}):
                    # Step 2: Perform binary search to determine the last page
                    total_pages = 5  # Start with an estimate
                    while True:
                        next_url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page={total_pages * 2}"
                        next_response = await self.fetch(session, next_url)
                        if next_response and "next" in next_response.get("_links", {}):
                            total_pages *= 2
                        else:
                            break

                    # Step 3: Refine total pages using binary search
                    low, high = total_pages // 2, total_pages
                    while low < high:
                        mid = (low + high + 1) // 2
                        test_url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page={mid}"
                        test_response = await self.fetch(session, test_url)
                        if test_response and "next" in test_response.get("_links", {}):
                            low = mid
                        else:
                            high = mid - 1

                    # Step 4: Sequentially verify additional pages
                    last_page = low
                    while True:
                        test_url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page={last_page + 1}"
                        test_response = await self.fetch(session, test_url)
                        if test_response:
                            last_page += 1
                        else:
                            break
                else:
                    # Single page only
                    last_page = 1

                print(f"Total number of pages for {endpoint}: {last_page}")

                # Step 5: Fetch all pages concurrently (if multiple pages exist)
                all_page_urls = [f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page={i}" for i in
                                 range(2, last_page + 1)]
                responses = await asyncio.gather(*(self.fetch(session, url) for url in all_page_urls))

                # Combine all responses
                all_responses = [first_response] + [res for res in responses if res]

                # Step 6: Save to JSON
                json.dump(all_responses, file, indent=4)
                print(f"Data saved to {filename}.json")

        end_time = time.time()
        print(
            f"End Time for {endpoint}: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')} - Total Time Taken: {end_time - start_time:.2f} seconds"
        )


    async def status_time(self,lead_id,data):
            for element in data:
                for index in element['_embedded']['leads']:
                    if index.get('id') == lead_id:  # Check if the lead matches the lead_id
                        lead = index  # Assign the actual lead data to `lead`
                        status = lead.get('custom_fields_values')  # Get the custom fields
                        if status:  # Check if status is not None
                            for field in status:
                                # Check if 'field_id' matches 1155527 and 'values' exists
                                if field.get('field_id') == 1155527 and 'values' in field:
                                    for value_entry in field['values']:
                                        # Extract and store the 'value'
                                        value = value_entry.get('value')
                                        if value:
                                            return self.convert_time(value)
    async def process_leads(self, data):
        leads_data = []

        # Helper to fetch the user, pipeline, and status concurrently
        async def fetch_lead_details(index):
            user_ids = [
                index.get("responsible_user_id"),
                index.get("created_by"),
                index.get("updated_by")
            ]
            pipeline_id = index.get("pipeline_id")
            status_id = index.get("status_id")


            # Concurrently fetch user, pipeline, and status details using cached functions
            user_results = await asyncio.gather(*[self.get_user(user_id) for user_id in user_ids])
            pipeline_name = await self.get_pipeline(pipeline_id)
            status_name = await self.get_status(pipeline_id, status_id)
            lead_status_time = await self.status_time(index.get("id"),data)

            lead = {
                "id": index.get("id"),
                "name": index.get("name"),
                "price": index.get("price"),
                "responsible_user_id": index.get("responsible_user_id"),
                "group_id": index.get("group_id"),
                "pipeline_id": index.get("pipeline_id"),
                "status_id": status_name,
                'status_time':lead_status_time,
                "updated_by": index.get("updated_by"),
                "created_at": self.convert_time(index.get("created_at")),
                "updated_at": self.convert_time(index.get("updated_at")),
                "closed_at":self.convert_time(index.get("closed_at")),
                "closest_task_at": self.convert_time(index.get("closest_task_at")),
                "is_deleted": index.get("is_deleted")
            }
            leads_data.append(lead)

        # Concurrently process all leads
        await asyncio.gather(
            *[fetch_lead_details(index) for element in data for index in element['_embedded']['leads']])
        return leads_data
        # df = pd.DataFrame(leads_data)
        # df.to_csv("Leads.csv", sep="|")

    def convert_time(self, timestamp):
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') if timestamp else None

    async def get_all_leads(self):
        with open("JSONS/leads.json", "r") as file:
            data = json.load(file)

            # Process the leads
            await self.process_leads(data)

    async def get_all_data(self):
        start_time = time.time()
        print(f"Overall Start Time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
        #
        await self.fetch_all_and_save("leads", "leads")
        await asyncio.sleep(5)  # Await sleep in an async function

        await self.fetch_all_and_save("users", "users")
        await asyncio.sleep(5)

        await self.fetch_all_and_save("pipelines", "leads/pipelines")



        await self.get_all_leads()

        end_time = time.time()
        print(
            f"Overall End Time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')} - Total Time Taken: {end_time - start_time:.2f} seconds")
from django.utils import timezone
import warnings

warnings.filterwarnings("ignore")
from asgiref.sync import sync_to_async
import time
from concurrent.futures import ThreadPoolExecutor
class Save_model(LeadProcessor):

    def __init__(self):
        super().__init__()

    def read_file(self, file_data):
        try:
            with open(file_data, "r") as data:
                return json.load(data)
        except FileNotFoundError:
            logger.warning(
                "File not found"
            )
    def save_users(self):
        start_time=time.time()
        data = self.read_file("JSONS/users.json")

        # Step 2: Prepare lists for new and existing users
        new_users = []
        existing_users = []

        # Step 3: Process the user data
        for element in data:
            for user in element['_embedded']['users']:
                user_id = user['id']
                user_name = user['name']

                # Step 4: Check if the user already exists in the database
                existing_user = Crm_users.objects.filter(user_id=user_id).first()

                if existing_user:

                    existing_user.name = user_name
                    existing_users.append(existing_user)
                else:
                    # If the user doesn't exist, create a new user and add to the new users list
                    new_user = Crm_users(user_id=user_id, name=user_name)
                    new_users.append(new_user)

        # Step 5: Perform bulk create and bulk update
        with transaction.atomic():  # Use transaction to ensure atomicity
            if new_users:
                Crm_users.objects.bulk_create(new_users)

            if existing_users:
                Crm_users.objects.bulk_update(existing_users, fields=['name'])
        print("Overaly total time taken:",time.time()-start_time)
        return f"Successfully saved {len(new_users)} new users and updated {len(existing_users)} users."

    def fetch_existing_pipelines(self, pipeline_ids):
        # Fetch all existing pipelines in one query
        return Pipeline.objects.filter(pipeline_id__in=pipeline_ids).values('id', 'pipeline_id')

    def save_pipelines_statuses(self):
        start_time = time.time()
        # Step 1: Read data from JSON
        data = self.read_file("JSONS/pipelines.json")

        # Step 2: Prepare lists and sets for efficient processing
        new_pipelines = []
        existing_pipelines = []
        pipeline_ids = set()  # To track pipeline IDs we need to check
        pipeline_name_map = {}  # Map pipeline IDs to their names for easy access
        status_map = {}  # Store statuses by pipeline
        all_status_ids = set()  # To track unique status_ids

        for element in data:
            for pipeline in element['_embedded']['pipelines']:
                pipeline_id = pipeline['id']
                pipeline_name = pipeline['name']
                pipeline_ids.add(pipeline_id)
                pipeline_name_map[pipeline_id] = pipeline_name

                # Process statuses
                for status in pipeline['_embedded']['statuses']:
                    status_id = status['id']
                    status_name = status['name']
                    if status_id not in all_status_ids:
                        all_status_ids.add(status_id)

                    if pipeline_id not in status_map:
                        status_map[pipeline_id] = []

                    status_map[pipeline_id].append({
                        'status_id': status_id,
                        'status_name': status_name
                    })

        # Step 4: Fetch existing pipelines and statuses from DB
        existing_pipelines_from_db = self.fetch_existing_pipelines(pipeline_ids)
        existing_pipeline_ids = {pipeline['pipeline_id'] for pipeline in existing_pipelines_from_db}
        pipeline_id_to_db_mapping = {pipeline['pipeline_id']: pipeline['id'] for pipeline in existing_pipelines_from_db}

        # Fetch all existing statuses to avoid duplicates
        existing_statuses_from_db = Status.objects.filter(status_id__in=all_status_ids)
        existing_status_ids = {status.status_id for status in existing_statuses_from_db}
        status_id_to_db_mapping = {status.status_id: status.id for status in existing_statuses_from_db}

        for pipeline_id, pipeline_name in pipeline_name_map.items():
            if str(pipeline_id) in existing_pipeline_ids:
                existing_pipelines.append(
                    Pipeline(
                        id=pipeline_id_to_db_mapping[str(pipeline_id)],  # Assign the primary key dynamically
                        pipeline_id=pipeline_id,  # Keep the original pipeline_id
                        pipeline_name=pipeline_name  # Update the name if needed
                    )
                )
            else:
                new_pipelines.append(Pipeline(pipeline_id=pipeline_id, pipeline_name=pipeline_name))

        # Step 6: Bulk create new pipelines and bulk update existing pipelines
        with transaction.atomic():  # Use transaction to ensure atomicity
            # Bulk create new pipelines
            if new_pipelines:
                Pipeline.objects.bulk_create(new_pipelines)

            # Bulk update existing pipelines (if needed)
            if existing_pipelines:
                Pipeline.objects.bulk_update(existing_pipelines, fields=['pipeline_name'])

        # Step 7: Process and save statuses (create only new unique statuses)
        new_statuses_to_create = []
        existing_statuses_to_update = []
        for records in status_map.values():
            for record in records:
                status_id = record['status_id']
                status_name = record['status_name']

                # Check if this combination (status_id, status_name) already exists in UniqueStatus
                if not UniqueStatus.objects.filter(status_id=status_id, status_name=status_name).exists():
                    # Save if not found
                    UniqueStatus.objects.create(status_id=status_id, status_name=status_name)

        for pipeline_id, statuses in status_map.items():
            # Find the associated pipeline from the database using filter
            pipelines = Pipeline.objects.filter(pipeline_id=pipeline_id)  # Use filter instead of get
            if pipelines.exists():
                # Use the first pipeline if multiple pipelines are returned
                pipeline_db_id = pipelines.first().id
                for status in statuses:
                    status_id = status['status_id']
                    status_name = status['status_name']

                    # Check if this status and pipeline combination already exists in the Status model
                    if not Status.objects.filter(status_id=status_id, pipeline_id=pipeline_db_id).exists():
                        # If it doesn't exist, create a new status
                        new_statuses_to_create.append(Status(
                            pipeline_id=pipeline_db_id,
                            status_id=status_id,
                            status_name=status_name
                        ))
                    else:
                        # If the combination already exists, update the status name if necessary
                        existing_status = Status.objects.get(status_id=status_id, pipeline_id=pipeline_db_id)
                        if existing_status.status_name != status_name:
                            existing_status.status_name = status_name
                            existing_statuses_to_update.append(existing_status)

        # Step 8: Bulk create new statuses and bulk update existing statuses
        with transaction.atomic():
            if new_statuses_to_create:
                Status.objects.bulk_create(new_statuses_to_create)
            if existing_statuses_to_update:
                Status.objects.bulk_update(existing_statuses_to_update, fields=['status_name'])

        total_time = time.time() - start_time
        return f"Total time taken: {total_time:.4f}s\nSuccessfully saved {len(new_pipelines)} new pipelines and {len(existing_pipelines)} updated pipelines.\nSuccessfully saved {len(new_statuses_to_create)} new statuses and updated {len(existing_statuses_to_update)} statuses."

    # async def save_leads_to_model(self, leads_data):
    #     # Get all existing lead_ids efficiently
    #     existing_leads = set(
    #         await sync_to_async(list)(
    #             Lead.objects.values_list('lead_id', flat=True)  # Retrieve existing lead IDs
    #         )
    #     )
    #
    #     new_leads = []
    #     updated_leads = []
    #
    #     # Process each lead and map to Lead model
    #     for lead_data in leads_data:
    #
    #         # Use sync_to_async to call these synchronous methods asynchronously
    #         responsible_user = await sync_to_async(Crm_users.objects.get)(user_id=lead_data["responsible_user_id"])
    #         pipeline = await sync_to_async(Pipeline.objects.get)(pipeline_id=lead_data["pipeline_id"])
    #         print("Pipeline", pipeline)
    #         print("Status_id", lead_data['status_id'])
    #         status = await sync_to_async(Status.objects.get)(pipeline=pipeline, status_id=lead_data["status_id"])
    #         print("Status", status)
    #
    #         lead_id = str(lead_data["id"])  # Make sure it's a string, as your logic suggests this is the format
    #
    #         if lead_id in existing_leads:
    #             try:
    #                 # If the lead exists, fetch and update its fields
    #                 lead_obj = await sync_to_async(Lead.objects.get)(lead_id=lead_id)
    #                 lead_obj.name = lead_data["name"]
    #                 lead_obj.price = lead_data["price"] or 0
    #                 lead_obj.responsible_user = responsible_user
    #                 lead_obj.pipeline = pipeline
    #                 lead_obj.status = status
    #                 lead_obj.is_deleted = lead_data["is_deleted"]
    #                 lead_obj.updated_at = lead_data['updated_at']
    #                 lead_obj.closed_at = lead_data['closed_at']
    #                 lead_obj.change_time_status = lead_data['status_time']
    #
    #                 updated_leads.append(lead_obj)  # Add to updated leads list
    #
    #             except Lead.DoesNotExist:
    #                 # This block might never be reached due to the above check (lead_id in existing_leads)
    #                 pass
    #
    #         else:
    #             # If the lead does not exist, create a new one
    #             lead_obj = Lead(
    #                 lead_id=lead_id,
    #                 name=lead_data["name"],
    #                 price=lead_data["price"] or 0,
    #                 responsible_user=responsible_user,
    #                 pipeline=pipeline,
    #                 status=status,
    #                 is_deleted=lead_data["is_deleted"],
    #                 created_at=lead_data['created_at'],
    #                 updated_at=lead_data['updated_at'],
    #                 closed_at=lead_data['closed_at'],
    #                 change_time_status=lead_data['status_time'],
    #             )
    #             new_leads.append(lead_obj)  # Add to new leads list
    #
    #     # Bulk create new leads
    #     if new_leads:
    #         await self.async_bulk_create(new_leads)
    #         print(f"Total leads created: {len(new_leads)}")
    #
    #     # Bulk update existing leads
    #     if updated_leads:
    #         await self.async_bulk_update(updated_leads, fields=[
    #             "name", "price", "responsible_user", "pipeline", "status", "is_deleted",
    #             "updated_at", "closed_at", "change_time_status"
    #         ])
    #         print(f"Total leads updated: {len(updated_leads)}")
    async def save_leads_to_model(self, leads_data):
        print(f"Leads data length: {len(leads_data)}")  # Check the length of leads_data

        # Get all existing lead_ids efficiently
        existing_leads = set(
            await sync_to_async(list)(
                Lead.objects.values_list('lead_id', flat=True)  # Retrieve existing lead IDs
            )
        )

        new_leads = []
        updated_leads = []

        async def process_lead(lead_data):
            try:
                print(f"Processing Lead: {lead_data['id']}")  # Check that the lead is being processed
                responsible_user = await sync_to_async(Crm_users.objects.get)(user_id=lead_data["responsible_user_id"])
                pipeline = await sync_to_async(Pipeline.objects.get)(pipeline_id=lead_data["pipeline_id"])
                status = await sync_to_async(Status.objects.get)(pipeline=pipeline, status_id=lead_data["status_id"])

                lead_id =str(lead_data["id"])
                print(f"Checking if Lead ID exists in existing_leads: {lead_id in existing_leads}")  # Debug check

                if lead_id in existing_leads:
                    try:
                        lead_obj = await sync_to_async(Lead.objects.get)(lead_id=lead_id)
                        print(f"Updating lead {lead_id}")
                        lead_obj.name = lead_data["name"]
                        lead_obj.price = lead_data["price"] or 0
                        lead_obj.responsible_user = responsible_user
                        lead_obj.pipeline = pipeline
                        lead_obj.status = status
                        lead_obj.is_deleted = lead_data["is_deleted"]
                        lead_obj.updated_at = lead_data['updated_at']
                        lead_obj.closed_at = lead_data['closed_at']
                        lead_obj.change_time_status = lead_data['status_time']

                        updated_leads.append(lead_obj)



                    except Lead.DoesNotExist:
                        print(f"Lead {lead_id} not found for update.")
                else:
                    print(f"Creating new lead {lead_id}")
                    lead_obj = Lead(
                        lead_id=lead_id,
                        name=lead_data["name"],
                        price=lead_data["price"] or 0,
                        responsible_user=responsible_user,
                        pipeline=pipeline,
                        status=status,
                        is_deleted=lead_data["is_deleted"],
                        created_at=lead_data['created_at'],
                        updated_at=lead_data['updated_at'],
                        closed_at=lead_data['closed_at'],
                        change_time_status=lead_data['status_time'],
                    )
                    new_leads.append(lead_obj)
            except Exception as e:
                print(f"Error processing lead {lead_data['id']}: {e}")

        if leads_data:
            print("Processing leads...")
            # Running all the tasks concurrently with asyncio.gather
            await asyncio.gather(*[process_lead(lead_data) for lead_data in leads_data])
        else:
            print("No leads to process.")

        print(f"Total new leads: {len(new_leads)}")
        print(f"Total updated leads: {len(updated_leads)}")

        if new_leads:
            await self.async_bulk_create(new_leads)
            print(f"Total leads created: {len(new_leads)}")

        if updated_leads:
            await self.async_bulk_update(updated_leads, fields=[
                "name", "price", "responsible_user", "pipeline", "status", "is_deleted",
                "updated_at", "closed_at", "change_time_status"
            ])
            print(f"Total leads updated: {len(updated_leads)}")


    async def async_bulk_create(self, objects, batch_size=100):
        for i in range(0, len(objects), batch_size):
            await asyncio.to_thread(Lead.objects.bulk_create, objects[i:i + batch_size])

    async def async_bulk_update(self, objects, fields, batch_size=100):
        for i in range(0, len(objects), batch_size):
            await asyncio.to_thread(Lead.objects.bulk_update, objects[i:i + batch_size], fields)


    async def save_leads(self):
        start_time=time.time()
        file = self.read_file("JSONS/leads.json")
        data = await super().process_leads(file)
        await self.save_leads_to_model(data)
        print("Total time:",time.time()-start_time)
def main():
    # model1=LeadProcessor()
    # asyncio.run(model1.get_all_data())
    model = Save_model()
    model.save_users()
    model.save_pipelines_statuses()
    asyncio.run(model.save_leads())

main()
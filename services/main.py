import requests
import time

import pandas as pd
import logging
from env import AMOCRM_SUBDOMAIN,ACCESS_TOKEN,API_URL
import django,os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()
import json,asyncio


def get_tokens():
    url = f"https://{AMOCRM_SUBDOMAIN}.amocrm.ru/oauth2/access_token"

    data = {
        "client_id": "9c594e67-3207-4ed8-aefa-d61e8590954f",  # Replace with your actual client_id
        "client_secret": "tDnX8ieNZwHmcK4J29nSYjk8fVYuNWeRXTQJvHlWHHDTA1eYcFbOSgVbehRzS7x3",  # Replace with your actual client_secret
        "grant_type": "authorization_code",
        "code": "def502004c53105e7793c20d152dffa971623e6bc3d94f61d846c03cfb5b580e5fa8262642a4cafa819cdcb9d67e4f7df4bd3d042a1a72bc3fde3710de8ff1d03df054b4764b5a4c41891e4f381c9f27552be42e96a22359ecacb727436bf62e011c8bf17dbf363fe11aa61aef95049b51f0dff62dd9bd8999eea3e13595711587d2f30ca5c8cb7c226941b491e47f2bd9619e4dbde7232e68ae488f8252676fdd73c28315d2de5a3b8a014f373699a480bff49c0f82e3fda70497e6fc8c8d31bb8e89367076407f17cb05e94fea246996a8d7cda12d7d5242c7e01bc17b179b6c1d39e307347f5ecbbaaa02e490204ad9b1ee506ed9864b44ac58eb1e9e1c839065b0e4e8b7c0f223bb938aa1395b92770ed8092b764e27d8e3bf215b8c4f84f0e6837449e717e24530d20f336ebf3f65339cce6319332a6044444f5edf03606eacc48b2fd0c3c0747db3b3f7bc2220e1ae634467ed48847c468dca01123686c72643cf3edd33aef8b43f0fcd11d3cae59f37b8b45f0a8dd095c72bfb37cdd59f0d22ebb95dce099e32ab39b607b8959a7e10a537353c03ddc2ab8f7a7ebc8c6fe7aea186059d44c5c55f529d13a369bffb4e79bbc735f1c7ac700f91bddc4337e6b4eb1d4ab79037c1caabb4a64d0d0fe9abf21c0dad391397e463d0035c3719",  # Replace with the authorization code from the URL
        "redirect_uri": "https://amocrm.ru"
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
# get_tokens()

import asyncio
import aiohttp
from datetime import timedelta,datetime

async def fetch(session, url):
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    try:
        async with session.get(url, headers=headers) as response:
            remaining_requests = int(response.headers.get('X-RateLimit-Remaining', 0))
            reset_time = int(response.headers.get('X-RateLimit-Reset', time.time()))

            if response.status == 200:
                return await response.json()  # Return the response as JSON
            elif response.status == 429:  # Rate limit exceeded
                # Calculate how long to wait based on the reset time
                wait_time = max(0, reset_time - time.time())
                print(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
                return await fetch(session, url)  # Retry the request
            else:
                print(f"Failed to get data from AmoCRM: {response.status}, {await response.text()}")
                return None
    except Exception as e:
        print(f"An error occurred while making the API request: {e}")
        return None


async def fetch_all_and_save(filename, endpoint):
    start_time = time.time()
    print(f"Start Time for {endpoint}: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")

    with open(f"JSONS/{filename}.json", "w") as file:
        async with aiohttp.ClientSession() as session:
            # Step 1: Fetch the first page
            first_page_url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page=1"
            print(first_page_url)
            first_response = await fetch(session, first_page_url)

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
                    next_response = await fetch(session, next_url)
                    if next_response and "next" in next_response.get("_links", {}):
                        total_pages *= 2
                    else:
                        break

                # Step 3: Refine total pages using binary search
                low, high = total_pages // 2, total_pages
                while low < high:
                    mid = (low + high + 1) // 2
                    test_url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page={mid}"
                    test_response = await fetch(session, test_url)
                    if test_response and "next" in test_response.get("_links", {}):
                        low = mid
                    else:
                        high = mid - 1

                # Step 4: Sequentially verify additional pages
                last_page = low
                while True:
                    test_url = f"https://pixeltechuz.amocrm.ru/api/v4/{endpoint}?page={last_page + 1}"
                    test_response = await fetch(session, test_url)
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
            responses = await asyncio.gather(*(fetch(session, url) for url in all_page_urls))

            # Combine all responses
            all_responses = [first_response] + [res for res in responses if res]

            # Step 6: Save to JSON
            json.dump(all_responses, file, indent=4)
            print(f"Data saved to {filename}.json")



async def get_pipeline(pipeline_id):

    with (open("../JSONS/pipelines.json", "r") as file):
        data = json.load(file)
        for element in data:
            for pipeline in element['_embedded']['pipelines']:
                if pipeline['id'] == pipeline_id:
                    return pipeline['name']


async def get_status(pipeline_id,status_id):
    with (open("../JSONS/pipelines.json", "r") as file):
        data = json.load(file)
        for element in data:
            for pipeline in element['_embedded']['pipelines']:
                if pipeline['id'] == pipeline_id:
                    for status in pipeline['_embedded']['statuses']:
                        if status['id']==status_id:
                            return status['name']


async def get_user(user_id):

    with (open("../JSONS/users.json", "r") as file):
        data = json.load(file)
        for element in data:
            for user in element['_embedded']['users']:
                if user['id']==user_id:
                    return user['name']
                return "Unknown"



def convert_time(timestamp):
    # If timestamp is None, return an empty string or a default value
    if timestamp is None:
        return "N/A"
    # Ensure the timestamp is a valid number before converting
    try:
        dt_object = datetime.fromtimestamp(int(timestamp))
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return "Invalid timestamp"

from datetime import datetime

async def status_time(lead_id):

    with open("../JSONS/leads.json", "r") as file:
        data = json.load(file)

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
                                        return convert_time(value)
async def get_all_leads():
    start_time = time.time()
    print(f"Start Time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    leads_data = []

    with open("../JSONS/leads.json", "r") as file:
        data = json.load(file)

        # Helper to fetch the user, pipeline, and status concurrently
        async def fetch_lead_details(index):
            user_ids = [
                index.get("responsible_user_id"),
                index.get("created_by"),
                index.get("updated_by")
            ]
            pipeline_id = index.get("pipeline_id")
            status_id = index.get("status_id")

            # Concurrently fetch user, pipeline, and status details
            user_results = await asyncio.gather(*[get_user(user_id) for user_id in user_ids])
            pipeline_name = await get_pipeline(pipeline_id)
            status_name = await get_status(pipeline_id, status_id)
            lead_status_time = await status_time(index.get('id'))  # Rename variable here

            lead = {
                "id": index.get("id"),
                "name": index.get("name"),
                "price": index.get("price"),
                "responsible_user_id": user_results[0],
                "group_id": index.get("group_id"),
                "pipeline_id": pipeline_name,
                "status_id": status_name,
                "created_by": user_results[1],
                "updated_by": user_results[2],
                "created_at": convert_time(index.get("created_at")),
                "updated_at": convert_time(index.get("updated_at")),
                "closed_at": convert_time(index.get("closed_at")),
                "closest_task_at": convert_time(index.get("closest_task_at")),
                "is_deleted": index.get("is_deleted"),
                # "status_time": lead_status_time  # Include this in the lead dictionary
            }
            leads_data.append(lead)

        # Concurrently process all leads
        await asyncio.gather(*[fetch_lead_details(index) for element in data for index in element['_embedded']['leads']])

    df = pd.DataFrame(leads_data)
    end_time = time.time()
    print(f"End Time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')} - Total Time Taken: {end_time - start_time:.2f} seconds")
    print(f"Total time taken:",end_time-start_time)
    df.to_csv("Leads.csv", sep="|")


async def get_all_datas():
    # Start time for overall function
    start_time = time.time()
    print(f"Generally Start Time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")

    await fetch_all_and_save("leads", "leads")
    await asyncio.sleep(5)  # Await sleep in an async function

    await fetch_all_and_save("users", "users")
    await asyncio.sleep(5)

    await fetch_all_and_save("pipelines", "leads/pipelines")

    # Start time for the get_all_leads function
    start_leads_time = time.time()

    # Call get_all_leads and measure time taken
    leads_result = await get_all_leads()

    # End time for the get_all_leads function
    end_leads_time = time.time()
    print(f"get_all_leads End Time: {datetime.fromtimestamp(end_leads_time).strftime('%Y-%m-%d %H:%M:%S')} - Total Time Taken: {end_leads_time - start_leads_time:.2f} seconds")

    # Overall end time
    end_time = time.time()
    print(f"Generally End Time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')} - Total Time Taken: {end_time - start_time:.2f} seconds")


    return leads_result  # Return the result after printing the time


# asyncio.run(get_all_datas())
# df=pd.read_csv("Leads.csv",sep="|")
# print(df)
asyncio.run(fetch_all_and_save("pipelines","leads/pipelines"))


# Run the function
# asyncio.run(status_time(19924597))
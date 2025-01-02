import requests
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta,datetime
from django.utils import timezone
import pandas as pd
import logging
from env import AMOCRM_SUBDOMAIN,ACCESS_TOKEN,API_URL
import django,os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()
from crm.models import Leads_status
from bot import problems
from requests.exceptions import HTTPError
import json,asyncio


# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class AmoCRM:
    def __init__(self):
        self.api_url = API_URL
        self.access_token = ACCESS_TOKEN
        self.last_sync_time = timezone.now() - timedelta(hours=1)  # Start with an initial sync time (1 hour ago)
        self.rate_limit_remaining = 100  # Example limit, update with actual API limits
        self.rate_limit_reset_time = 0

    def get_amo_data(self, url, headers, params=None):
        try:
            while self.rate_limit_remaining <= 0:
                logger.warning("Rate limit exceeded. Waiting for reset...")
                time.sleep(self.rate_limit_reset_time)

            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                self.rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", self.rate_limit_remaining))
                self.rate_limit_reset_time = int(response.headers.get("Retry-After", 1))
                return response.json()

            elif response.status_code == 204:
                logger.info("No content returned from AmoCRM API.")
                message = "No content returned from AmoCRM API."
                asyncio.run(problems(message))


            elif response.status_code == 429:  # Rate-limiting
                self.rate_limit_reset_time = int(response.headers.get("Retry-After", 5))
                logger.warning("Rate limit hit. Retrying after delay...")
                message = "Rate limit hit. Retrying after delay..."
                asyncio.run(problems(message))
                time.sleep(self.rate_limit_reset_time)
                return self.get_amo_data(url, headers, params)
            elif response.status_code == 401:
                logger.warning("Received 401 Unauthorized. Attempting to refresh token...")
                message = "Received 401 Unauthorized. Attempting to refresh token..."
                asyncio.run(problems(message))

                new_access_token = self.refresh_tokens()  # Refresh token and get the new one
                if new_access_token:
                    self.access_token = new_access_token  # Update the token in the class instance
                    headers['Authorization'] = f'Bearer {self.access_token}'  # Update headers
                    return self.get_amo_data(url, headers, params)  # Retry the request with the new token
                else:
                    logger.error("Failed to refresh the token. Aborting request.")
                    asyncio.run(problems("The refresh token is invalid, expired, or revoked. Please re-authenticate.Or please click /tokens.You have 10 minutes"))
                    time.sleep(600)
                    self.get_tokens()
            else:
                logger.error(f"Failed to get data from AmoCRM: {response.status_code}, {response.text}")
                return None
        except requests.RequestException as e:
            logger.error(f"An error occurred while making the API request: {e}")
            return None

    def get_tokens(self):
        url = f"https://{AMOCRM_SUBDOMAIN}.amocrm.ru/oauth2/access_token"
        with open("../tokens_file.json", mode="r") as json_file:
            tokens_data = json.load(json_file)
        data = {
            "client_id": tokens_data.get("client_id"),  # Fetching from loaded JSON
            "client_secret": tokens_data.get("client_secret"),  # Fetching from loaded JSON
            "grant_type": "authorization_code",
            "code": tokens_data.get("code"),  # Fetching from loaded JSON
            "redirect_uri": tokens_data.get("redirect_uri")  # Fetching from loaded JSON
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            tokens = response.json()
            with open("../access_token.txt", "w") as access_file:
                access_file.write(tokens['access_token'])

            with open("../refresh_token.txt", "w") as refresh_file:
                refresh_file.write(tokens['refresh_token'])
            print("Tokens obtained successfully:", tokens)
            asyncio.run(problems("Successfully created access and refresh tokens☺️"))
        else:
            print(f"Failed to obtain tokens: {response.status_code}, {response.text}")

    def refresh_tokens(self):
        url = f"https://{AMOCRM_SUBDOMAIN}.amocrm.ru/oauth2/access_token"
        with open("../tokens_file.json", mode="r") as json_file:
            tokens_data = json.load(json_file)

        with open("../refresh_token.txt", "r") as refresh_file:
            refresh_token = refresh_file.read().strip()

        data = {
            "client_id": tokens_data.get("client_id"),
            "client_secret": tokens_data.get("client_secret"),
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri": tokens_data.get("redirect_uri"),
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            tokens = response.json()
            with open("../access_token.txt", "w") as access_file:
                access_file.write(tokens['access_token'])

            with open("../refresh_token.txt", "w") as refresh_file:
                refresh_file.write(tokens['refresh_token'])
            print("Tokens refreshed successfully:", tokens)
            asyncio.run(problems("Successfully refreshed access and refresh tokens☺️"))
            return tokens['access_token']

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
        print(f"Start Time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")

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
            return None

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
            i = 0
            # Fetch additional fields for each lead (responsible_user, created_by, pipeline, and status)
            for index, lead in leads_df.iterrows():
                responsible_user_data = self.get_amo_data(f"{self.api_url}/users/{lead['responsible_user_id']}", headers)
                created_by_data = self.get_amo_data(f"{self.api_url}/users/{lead['created_by']}", headers) if lead['created_by'] != 0 else {"name": "Unknown"}
                pipeline_data = self.get_amo_data(f"{self.api_url}/leads/pipelines/{lead['pipeline_id']}", headers)
                status_data = self.get_amo_data(f"{self.api_url}/leads/pipelines/{lead['pipeline_id']}/statuses/{lead['status_id']}", headers)

                # Update the DataFrame with the additional fetched data
                leads_df.at[index, 'responsible_user_id'] = responsible_user_data.get("name", "Unknown")
                leads_df.at[index, 'created_by'] = created_by_data.get("name", "Unknown")
                leads_df.at[index, 'pipeline_id'] = pipeline_data.get("name", "Unknown")
                leads_df.at[index, 'status_id'] = status_data.get("name", "Unknown")
                i += 1
                print(f"Processed Lead {i}: {lead['lead_id']}")
                asyncio.run(problems(f"Processed Lead {i}: {lead['lead_id']}"))
            all_leads.append(leads_df)

        next_page_url = data.get("_links", {}).get("next", {}).get("href")
        print("Next page URL:", next_page_url)

        # Collect all next page URLs for parallel fetching
        # all_urls = []
        # while next_page_url:
        #     all_urls.append(next_page_url)
        #     data = self.get_amo_data(next_page_url, headers)
        #     next_page_url = data.get("_links", {}).get("next", {}).get("href")
        #
        # # Use ThreadPoolExecutor to fetch all the pages in parallel
        # with ThreadPoolExecutor(max_workers=10) as executor:
        #     results = list(executor.map(self.get_amo_data, all_urls, [headers] * len(all_urls)))
        #
        # for result in results:
        #     if result:
        #         leads = result.get("_embedded", {}).get("leads", [])
        #         if leads:
        #             leads_df = pd.json_normalize(leads)[list(key_mapping.keys())]
        #             leads_df.rename(columns=key_mapping, inplace=True)
        #             leads_df = leads_df.astype({
        #                 'responsible_user_id': 'object',
        #                 'created_by': 'object',
        #                 'pipeline_id': 'object',
        #                 'status_id': 'object'
        #             })
        #             i=0
        #             for index, lead in leads_df.iterrows():
        #                 responsible_user_data = self.get_amo_data(f"{self.api_url}/users/{lead['responsible_user_id']}", headers)
        #                 created_by_data = self.get_amo_data(f"{self.api_url}/users/{lead['created_by']}", headers) if lead['created_by'] != 0 else {"name": "Unknown"}
        #                 pipeline_data = self.get_amo_data(f"{self.api_url}/leads/pipelines/{lead['pipeline_id']}", headers)
        #                 status_data = self.get_amo_data(f"{self.api_url}/leads/pipelines/{lead['pipeline_id']}/statuses/{lead['status_id']}", headers)
        #
        #                 leads_df.at[index, 'responsible_user'] = responsible_user_data.get("name", "Unknown")
        #                 leads_df.at[index, 'created_by'] = created_by_data.get("name", "Unknown")
        #                 leads_df.at[index, 'pipeline'] = pipeline_data.get("name", "Unknown")
        #                 leads_df.at[index, 'status'] = status_data.get("name", "Unknown")
        #                 i+=1
        #                 print(f"Processed Lead {i}: {lead['lead_id']}")
        #             all_leads.append(leads_df)

        # Combine all DataFrames
        if all_leads:
            final_df = pd.concat(all_leads, ignore_index=True)
            self.last_sync_time = timezone.now()
            # Record the end time
            end_time = time.time()
            print(f"End Time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Total Time Taken: {end_time - start_time:.2f} seconds")
            return final_df
        else:
            print("No leads data found.")
            return None


class SaveModel:
    def __init__(self, data):
        self.data = data

    def tomodel(self):
        leads_to_create = []
        leads_to_update = []
        start_time = time.time()
        logger.info("Processing leads...")

        try:
            existing_leads = Leads_status.objects.filter(
                lead_id__in=self.data['lead_id'],
            ).values('lead_id', 'status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline')

            existing_leads_map = {lead['lead_id']: lead for lead in existing_leads}
            ids = [h for h in existing_leads_map.keys()]
            print("Ids:",ids)
            for _, row in self.data.iterrows():
                lead_id = row['lead_id']
                if str(lead_id) in ids:
                    existing_lead = existing_leads_map[str(lead_id)]
                    # Check if update is needed
                    if any(
                        existing_lead[field] != row.get(field, None)
                        for field in ['status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline']
                    ):
                        lead_obj = Leads_status.objects.get(lead_id=lead_id)
                        for field in ['status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline']:
                            setattr(lead_obj, field, row.get(field, None))
                        leads_to_update.append(lead_obj)
                else:

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
                            last_time_sync=timezone.now(),
                        )
                    )

            # Perform bulk operations
            with ThreadPoolExecutor(max_workers=2) as executor:
                if leads_to_create:
                    executor.submit(self.bulk_create_leads, leads_to_create)
                if leads_to_update:
                    executor.submit(self.bulk_update_leads, leads_to_update)

        except Exception as e:
            logger.error(f"Error processing leads: {e}")
            message = f"Error processing leads: {e}"
            asyncio.run(problems(message))
        finally:
            logger.info(f"Processing complete. Time taken: {time.time() - start_time:.2f} seconds.")

    def bulk_create_leads(self, leads_to_create):
        Leads_status.objects.bulk_create(leads_to_create, batch_size=100)
        logger.info(f"Created {len(leads_to_create)} leads.")
        message = f"Created {len(leads_to_create)} leads."
        asyncio.run(problems(message))
    def bulk_update_leads(self, leads_to_update):
        Leads_status.objects.bulk_update(leads_to_update, fields=[
            'status', 'price', 'lead_name', 'responsible_user', 'created_by', 'group', 'pipeline', 'last_time_sync'
        ], batch_size=100)
        logger.info(f"Updated {len(leads_to_update)} leads.")
        message = f"Updated {len(leads_to_update)} leads."
        asyncio.run(problems(message))


def fetch_and_process_data(page_number):
    amocrm = AmoCRM()
    logger.info(f"Fetching data for page {page_number}")
    asyncio.run(problems(f"Fetching data for page {page_number}"))

    # Fetch data from the API
    data = amocrm.get_all_leads(endpoint=f"leads?page={page_number}")

    if data is not None:
        # Process the data
        savemodel = SaveModel(data=data)
        savemodel.tomodel()
        return True  # Indicates that data was found and processed
    else:
        logger.warning(f"No data found on page {page_number}.")
        return False  # No data found, should stop fetching further pages


if __name__ == "__main__":
    page_number = 1  # Start from the first page
    while True:
        try:
            # Try to fetch and process data for the current page
            data_found = fetch_and_process_data(page_number)

            if data_found:
                page_number += 1  # Increment the page number after successful data processing
            else:
                # If no data is found, start again from page 1
                logger.info(f"Restarting from page 1 after page {page_number}")
                page_number = 1  # Reset to page 1 after reaching an empty page

            # Sleep to respect API rate limits
            time.sleep(60)

        except HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"Page {page_number} returned 404. Moving to the next page.")
                page_number += 1

            else:
                logger.error(f"HTTP error occurred: {e}")
        except KeyboardInterrupt:
            logger.info("Exiting program.")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")



import requests
import psycopg2
from time import sleep
AMOCRM_SUBDOMAIN = 'pixeltechuz'  # Replace with your AmoCRM subdomain
ACCESS_TOKEN = open("access_token.txt",mode="r",encoding="UTF-8").read().strip()
API_URL = f'https://{AMOCRM_SUBDOMAIN}.amocrm.ru/api/v4'



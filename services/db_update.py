
import os,requests,time,datetime
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()
from crm.models import Lead
# def get_postgres_memory_usage(dbname, user, password, host, port=5432):
#     try:
#         # Connect to the PostgreSQL database
#         connection = psycopg2.connect(
#             dbname="mypostgres1_amocrmdb",
#             user="mypostgres1",
#             password="2004postgres",
#             host="postgresql-mypostgres1.alwaysdata.net",
#             port=port
#         )
#
#         with connection.cursor(cursor_factory=RealDictCursor) as cursor:
#
#             cursor.execute(f"SELECT pg_database_size('{dbname}') AS size_bytes;")
#             db_size = cursor.fetchone()
#
#             if db_size:
#                 size_bytes = db_size["size_bytes"]
#                 size_mb = size_bytes / (1024 * 1024)  # Convert to MB
#                 return f"Database size: {size_mb:.2f} MB"
#             else:
#                 return "Unable to fetch database size."
#
#     except Exception as e:
#         return f"Error: {str(e)}"
#     finally:
#         # Close the connection
#         if connection:
#             connection.close()

# from django.db.models import Min
#
# #
# duplicates = (
#     Lead.objects.values('lead_id')
#     .annotate(min_id=Min('id'))
#     .values_list('lead_id', 'min_id')
# )
#
# # Get all IDs except the ones to retain
# ids_to_retain = [item[1] for item in duplicates]
# duplicates_to_delete = Lead.objects.exclude(id__in=ids_to_retain)
#
# # Delete duplicates
# duplicates_to_delete.delete()
# print(f"Removed {duplicates_to_delete.count()} duplicate entries.")

# from django.utils.timezone import make_aware
# from datetime import datetime
#
# # Correct the format to match the datetime string
# specific_time = make_aware(datetime.strptime("2024-12-02 09:39:42", "%Y-%m-%d %H:%M:%S"))
#
# # Delete records where last_time_sync is not equal to the specific time
# Lead.objects.exclude(last_time_sync=specific_time).delete()
#
# print("Records with last_time_sync not equal to 2024-12-02 09:39:42 have been deleted.")

from datetime import datetime
s=datetime.fromtimestamp(946688461)
print(s)
print(s.isoformat())
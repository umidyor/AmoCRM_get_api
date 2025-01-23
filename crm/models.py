from django.db import models
# class Leads_status(models.Model):
#     lead_id=models.CharField(max_length=100)
#     lead_name=models.CharField(max_length=255)
#     responsible_user=models.CharField(max_length=255)
#     created_by=models.CharField(max_length=100)
#     group = models.IntegerField()
#     price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
#     pipeline=models.CharField(max_length=255)
#     status=models.CharField(max_length=255)
#     last_time_sync = models.DateTimeField(null=True, blank=True)
#
#     def __str__(self):
#         return self.lead_name

from django.utils import timezone
from datetime import datetime
# Utility function to convert Unix timestamp to datetime
def unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp)

class Pipeline(models.Model):
    pipeline_id=models.CharField(max_length=255)
    pipeline_name = models.CharField(max_length=255)

    def __str__(self):
    	return str(self.id)

class Status(models.Model):
    pipeline=models.ForeignKey(Pipeline,on_delete=models.CASCADE,null=True)
    status_id=models.CharField(max_length=255)
    status_name = models.CharField(max_length=255)
    def __str__(self):
        return str(self.id)
class UniqueStatus(models.Model):
    status_id=models.CharField(max_length=255)
    status_name=models.CharField(max_length=255)
    def __str__(self):
        return str(self.id)

class Crm_users(models.Model):
    user_id=models.CharField(max_length=255)
    name=models.CharField(max_length=255)

    def __str__(self):
        return str(self.id)

class Lead(models.Model):
    lead_id=models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    price = models.IntegerField()
    responsible_user = models.ForeignKey(Crm_users,on_delete=models.SET_NULL,null=True)
    pipeline = models.ForeignKey(Pipeline, on_delete=models.CASCADE)
    status = models.ForeignKey(Status, on_delete=models.CASCADE)
    is_deleted = models.BooleanField(default=False)
    created_at = models.DateTimeField(null=True)
    updated_at = models.DateTimeField(null=True)
    closed_at = models.DateTimeField(null=True)
    lead_active_time=models.CharField(max_length=255)
    change_time_status=models.DateTimeField(default=datetime(1111, 1, 1, 00, 00, 0),null=True, blank=True)
    def __str__(self):
        return str(self.id)

    def calculate_status_duration(self, old_status_time, new_status_time):
        if old_status_time and new_status_time:
            if isinstance(old_status_time, str):
                old_status_time = datetime.fromisoformat(old_status_time)
            if isinstance(new_status_time, str):
                new_status_time = datetime.fromisoformat(new_status_time)

            # Calculate the duration in seconds
            duration = new_status_time - old_status_time
            total_seconds = duration.total_seconds()

            # Convert total seconds to days, hours, minutes, and seconds
            days = total_seconds // (24 * 3600)
            hours = (total_seconds % (24 * 3600)) // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60

            # Return a tuple (days, hours, minutes, seconds)
            return f"Days:{days} Hours:{hours}, Minutes:{minutes}, Seconds:{seconds}"
        return 0


class Lead_history(models.Model):
    lead_id=models.ForeignKey(Lead,on_delete=models.CASCADE,null=True)
    old_status = models.ForeignKey(Status, on_delete=models.CASCADE, related_name='old_status')
    old_status_time = models.DateTimeField()
    new_status = models.ForeignKey(Status, on_delete=models.CASCADE, related_name='new_status')
    new_status_time = models.DateTimeField()
    total_time_status=models.CharField(max_length=500)
    old_pipeline = models.ForeignKey(Pipeline, on_delete=models.CASCADE, related_name='old_pipeline')
    new_pipeline = models.ForeignKey(Pipeline, on_delete=models.CASCADE, related_name='new_pipeline')

    def __str__(self):
        return f"History for {self.lead_id.lead_id} from {self.old_status} to {self.new_status}"

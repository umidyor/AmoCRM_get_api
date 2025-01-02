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
        return self.pipeline_name

class Status(models.Model):
    pipeline=models.ForeignKey(Pipeline,on_delete=models.CASCADE,null=True)
    status_id=models.CharField(max_length=255)
    status_name = models.CharField(max_length=255)
    def __str__(self):
        return self.status_name
class UniqueStatus(models.Model):
    status_id=models.CharField(max_length=255)
    status_name=models.CharField(max_length=255)
    def __str__(self):
        return self.status_name

class Crm_users(models.Model):
    user_id=models.CharField(max_length=255)
    name=models.CharField(max_length=255)

    def __str__(self):
        return self.name
class Lead(models.Model):
    lead_id=models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    price = models.IntegerField()
    responsible_user = models.ForeignKey(Crm_users,on_delete=models.SET_NULL,null=True)
    pipeline = models.ForeignKey(Pipeline, on_delete=models.CASCADE)
    status = models.ForeignKey(Status, on_delete=models.CASCADE)
    is_deleted = models.BooleanField(default=False)

    # Use datetime fields instead of Unix timestamps
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    closed_at = models.DateTimeField(null=True, blank=True)
    change_time_status=models.DateTimeField(null=True)
    def __str__(self):
        return self.lead_id

    def update_status_and_pipeline(self, new_status, new_pipeline,change_time_status):
        """
        Updates the lead's status and pipeline, and creates a history record
        for the change.
        """

        old_status = self.status
        old_pipeline = self.pipeline
        old_status_time = self.change_time_status

        self.status = new_status
        self.pipeline = new_pipeline
        self.change_time_status = change_time_status
        self.save()

        # Create a history entry for the change
        Lead_history.objects.create(
            lead_id=self,
            old_status=old_status,
            old_status_time=old_status_time,
            new_status=self.status,
            new_status_time=self.change_time_status,
            total_time_status=self.calculate_status_duration(old_status_time, self.change_time_status),
            old_pipeline=old_pipeline,
            new_pipeline=self.pipeline,
        )

    def calculate_status_duration(self, old_status_time, new_status_time):
        """
        Calculate the duration for which the status was active.
        """
        if old_status_time and new_status_time:
            duration = new_status_time - old_status_time  # Calculate the time difference
            return duration.total_seconds()  # Return the total time in seconds
        return 0  # Return 0 if no previous status time exists


class Lead_history(models.Model):
    lead_id=models.ForeignKey(Lead,on_delete=models.CASCADE,null=True)
    old_status = models.ForeignKey(Status, on_delete=models.CASCADE, related_name='old_status')
    old_status_time = models.DateTimeField(null=True,blank=True)
    new_status = models.ForeignKey(Status, on_delete=models.CASCADE, related_name='new_status')
    new_status_time = models.DateTimeField(null=True,blank=True)
    total_time_status=models.DateTimeField()
    old_pipeline = models.ForeignKey(Pipeline, on_delete=models.CASCADE, related_name='old_pipeline')
    new_pipeline = models.ForeignKey(Pipeline, on_delete=models.CASCADE, related_name='new_pipeline')

    def __str__(self):
        return f"History for {self.lead_id.lead_id} from {self.old_status} to {self.new_status}"
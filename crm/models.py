from django.db import models
class Leads_status(models.Model):
    lead_id=models.CharField(max_length=100)
    lead_name=models.CharField(max_length=255)
    responsible_user=models.CharField(max_length=255)
    created_by=models.CharField(max_length=100)
    group = models.IntegerField()
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    pipeline=models.CharField(max_length=255)
    status=models.CharField(max_length=255)
    last_time_sync = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.lead_name
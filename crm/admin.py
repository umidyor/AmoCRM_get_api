from django.contrib import admin
from .models import *

@admin.register(Leads_status)
class AdminLead(admin.ModelAdmin):
    list_display=['lead_id','lead_name','pipeline','status']
    search_fields=['lead_name','lead_id','pipeline','status']

# Register your models here.

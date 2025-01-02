from django.contrib import admin
from .models import *

@admin.register(Lead)
class AdminLead(admin.ModelAdmin):
    list_display=['lead_id','name','pipeline','status']
    search_fields=['name','pk']


admin.site.register(Crm_users)
admin.site.register(Lead_history)

@admin.register(UniqueStatus)
class AdminUniqueStatus(admin.ModelAdmin):
    list_display = ['status_name','status_id']
    search_fields = ['status_name','status_id']
@admin.register(Status)
class AdminUniqueStatus(admin.ModelAdmin):
    list_display = ['status_name','status_id']
    search_fields = ['status_name','status_id']
@admin.register(Pipeline)
class AdminPipeline(admin.ModelAdmin):
    list_display = ['pipeline_name','pipeline_id','pk']
    search_fields = ['pipeline_name','pipeline_id']
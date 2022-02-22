from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import Price

class atlas_admin(UserAdmin):   
    list_display = ('report_date', 'location','instrument', 'date', 'value', 'freq')
    
    fieldsets = (
        (None, {
            'fields' : ('report_date', 'location','instrument', 'date', 'value', 'freq')
        })
    )
    
    add_fieldsets = (
        (None, {
            'fields' : ('report_date', 'location','instrument', 'date', 'value', 'freq')
        })
    )
    
admin.site.register(Price)
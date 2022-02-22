from django.db import models

# Create your models here.
class Price(models.Model):     
    report_date = models.DateField()
    location = models.CharField(max_length=10)
    instrument = models.CharField(max_length=10)
    date =  models.DateField()
    value = models.FloatField()
    freq = models.CharField(max_length=5)
    
    class Meta:     
        db_table = 'Price'
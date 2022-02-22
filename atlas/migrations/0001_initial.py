# Generated by Django 3.2.12 on 2022-02-11 05:51

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Price',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('report_date', models.DateField()),
                ('location', models.CharField(max_length=10)),
                ('instrument', models.CharField(max_length=10)),
                ('date', models.DateField()),
                ('value', models.FloatField()),
                ('freq', models.CharField(max_length=5)),
            ],
        ),
    ]

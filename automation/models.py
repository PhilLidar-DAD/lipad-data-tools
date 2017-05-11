#!/usr/bin/env python

import peewee
from settings import *


PSQL_DB = peewee.PostgresqlDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


class BaseModel(peewee.Model):

    class Meta:
        database = PSQL_DB


class Automation_AutomationJob(BaseModel):
    id = peewee.IntegerField(primary_key=True)
    datatype = peewee.CharField(max_length=10)
    input_dir = peewee.TextField(null=False)
    output_dir = peewee.TextField(null=False)
    processor = peewee.CharField(max_length=10)
    date_submitted = peewee.DateTimeField(null=False)
    status = peewee.CharField(max_length=20)
    status_timestamp = peewee.DateTimeField(null=True)
    target_os = peewee.CharField(max_length=20)
    log = peewee.TextField(null=False)

    # class Meta:
    #     primary_key = peewee.CompositeKey(
    #         'datatype', 'date_submitted', 'status')


class Cephgeo_LidarCoverageBlock(BaseModel):
    """
        From geonode.cephgeo.models LidarCoverageBlock
        Only UID and Block Name needed for renaming tiles
    """
    uid = peewee.IntegerField(primary_key=True)
    block_name = peewee.CharField()

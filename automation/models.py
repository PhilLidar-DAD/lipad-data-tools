#!/usr/bin/env python

import peewee
from settings import *
from peewee import *


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


class CephDataObject(BaseModel):
    id = peewee.IntegerField(primary_key=True)
    size_in_bytes = peewee.IntegerField()
    file_hash = peewee.CharField(max_length=40)
    name = peewee.CharField(max_length=100)
    last_modified = peewee.DateTimeField()
    content_type = peewee.CharField(max_length=20)
    data_class = peewee.CharField(max_length=20)
    grid_ref = peewee.CharField(max_length=10)


class DataClassification(Field):
    UNKNOWN = 0
    LAZ = 1
#    DEM = 2
    DTM = 3
    DSM = 4
    ORTHOPHOTO = 5

    labels = {
        UNKNOWN: "Unknown Type",
        LAZ: "LAZ",
        #        DEM         : "DEM TIF",
        DSM: "DSM TIF",
        DTM: "DTM TIF",
        ORTHOPHOTO: "Orthophoto", }

    gs_feature_labels = {
        UNKNOWN: "UNSUPPORTED",
        LAZ: "LAZ",
        #        DEM         : "UNSUPPORTED",
        DSM: "DSM",
        DTM: "DTM",
        ORTHOPHOTO: "ORTHO", }

    filename_suffixes = {
        ".laz": LAZ,
        #        "_dem.tif"         : DEM,
        "_dsm.tif": DSM,
        "_dtm.tif": DTM,
        "_ortho.tif": ORTHOPHOTO, }

class Automation_Demdatastore(BaseModel):
    demid = PrimaryKeyField()
    block_name_list = TextField()
    dem_file_path = TextField()
    height_diff = FloatField()
    name = CharField()
    rmse = FloatField()
    shifting_val_x = FloatField()
    shifting_val_y = FloatField()
    shifting_val_z = FloatField()
    suc = CharField()
    type = CharField()

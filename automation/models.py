#!/usr/bin/env python

import peewee
from settings import *
from peewee import *


PSQL_DB = peewee.PostgresqlDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


class BaseModel(peewee.Model):

    class Meta:
        database = PSQL_DB


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


class Automation_AutomationJob(BaseModel):
    """Inherit ``BaseModel`` `model`.

    A model interface for geonode.automation.models.AutomationJob.

    Attributes:
        id (int): Corresponds to `model` ``AutomationJob`` identifier in database.
        datatype (str): Corresponds to `model` ``AutomationJob`` datatype
            in database.
        input_dir (str): Corresponds to `model` ``AutomationJob`` input directory
            in database.
        output_dir (str): Corresponds to `model` ``AutomationJob`` output directory
            in database.
        processor (str): Corresponds to `model` ``AutomationJob`` processor
            in database.
        date_submitted (date): Corresponds to `model` ``AutomationJob`` date_submitted
            in database.
        target_os (str): Corresponds to `model` ``AutomationJob`` target_os
            in database.
        log (str): Corresponds to `model` ``AutomationJob`` log in database.

    """
    STATUS_CHOICES = [
        ('pending_process'),    # Pending Job
        ('done_process'),       # Processed Job
        ('pending_ceph'),       # Uploading in Ceph
        ('done_ceph'),          # Uploaded in Ceph
        ('done'),               # Uploaded in LiPAD
        # (-1, 'error', _('Error')),
    ]
    # ('pending_process', _('Pending Job')),
#         # ('done_process', _('Processing Job')),
#         ('done_process', _('Processed Job')),
#         ('pending_ceph', _('Uploading in Ceph')),
#         ('done_ceph', _('Uploaded in Ceph')),
#         ('done', _('Uploaded in LiPAD')),
    OS_CHOICES = [
        ('linux', ('Process in Linux')),
        ('windows', ('Process in Windows')),
    ]

    id = peewee.IntegerField(primary_key=True)
    datatype = peewee.CharField(max_length=10)
    input_dir = peewee.TextField(null=False)
    output_dir = peewee.TextField(null=False)
    processor = peewee.CharField(max_length=10)
    date_submitted = peewee.DateTimeField(null=False)
    # status = peewee.CharField(max_length=20)
    status = peewee.CharField(choices=STATUS_CHOICES)
    status_timestamp = peewee.DateTimeField(null=True)
    target_os = peewee.CharField(choices=OS_CHOICES)
    # target_os = OSChoice(choices=OS_CHOICES)
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


class Cephgeo_DemDataStore():
    demid = peewee.IntegerField(primary_key=True)
    name = peewee.CharField(max_length=20)
    suc = peewee.CharField(max_length=5)
    type = peewee.CharField(max_length=5)
    shifting_val_x = peewee.FloatField()
    shifting_val_y = peewee.FloatField()
    shifting_val_z = peewee.FloatField()
    height_diff = peewee.FloatField()
    rmse = peewee.FloatField()
    dem_file_path = peewee.TextField(null=False)
    block_name_list = peewee.TextField(null=False)

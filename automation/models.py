#!/usr/bin/env python

import peewee
from settings import *
from peewee import *


PSQL_DB = PostgresqlDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


class BaseModel(Model):

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
        # ('pending_ceph'),       # Uploading in Ceph
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

    id = IntegerField(primary_key=True)
    datatype = CharField(max_length=10)
    input_dir = TextField(null=False)
    output_dir = TextField(null=False)
    processor = CharField(max_length=10)
    date_submitted = DateTimeField(null=False)
    # status = CharField(max_length=20)
    status = CharField(choices=STATUS_CHOICES)
    status_timestamp = DateTimeField(null=True)
    target_os = CharField(choices=OS_CHOICES)
    # target_os = OSChoice(choices=OS_CHOICES)
    data_processing_log = TextField(null=False)
    ceph_upload_log = TextField(null=False)
    database_upload_log = TextField(null=False)

    # class Meta:
    #     primary_key = CompositeKey(
    #         'datatype', 'date_submitted', 'status')


class Cephgeo_LidarCoverageBlock(BaseModel):
    """
        From geonode.cephgeo.models LidarCoverageBlock
        Only UID and Block Name needed for renaming tiles
    """
    uid = IntegerField(primary_key=True)
    block_name = CharField()


class Cephgeo_CephDataObject(BaseModel):
    id = IntegerField(primary_key=True)
    size_in_bytes = IntegerField()
    file_hash = CharField(max_length=40)
    name = CharField(max_length=100)
    last_modified = DateTimeField()
    content_type = CharField(max_length=20)
    data_class = CharField(max_length=20)
    grid_ref = CharField(max_length=10)
    block_uid = ForeignKeyField(Cephgeo_LidarCoverageBlock)


class Cephgeo_DemDataStore():
    demid = IntegerField(primary_key=True)
    name = CharField(max_length=20)
    suc = CharField(max_length=5)
    type = CharField(max_length=5)
    shifting_val_x = FloatField()
    shifting_val_y = FloatField()
    shifting_val_z = FloatField()
    height_diff = FloatField()
    rmse = FloatField()
    dem_file_path = TextField(null=False)
    block_name_list = TextField(null=False)

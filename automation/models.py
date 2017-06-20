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

    STATUS_CHOICES = [
        ('pending_process'),
        ('done_process'),
        ('pending_ceph'),
        ('done_ceph'),
        ('done'),
        # (-1, 'error', _('Error')),
    ]

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

class Automation_Demdatastore(BaseModel):
    block_name_list = TextField()
    dem_file_path = TextField()
    demid = PrimaryKeyField()
    height_diff = FloatField()
    name = CharField()
    rmse = FloatField()
    shifting_val_x = FloatField()
    shifting_val_y = FloatField()
    shifting_val_z = FloatField()
    suc = CharField()
    type = CharField()

    class Meta:
        db_table = 'automation_demdatastore'

class Cephgeo_LidarCoverageBlock(BaseModel):
    """
        From geonode.cephgeo.models LidarCoverageBlock
        Only UID and Block Name needed for renaming tiles
    """
    adjusted_l = TextField()
    block_name = CharField(index=True)
    cal_ref_pt = TextField()
    date_flown = DateField(null=True)
    flight_num = TextField()
    floodplain = TextField()
    height_dif = TextField()
    mission_na = TextField()
    pl1_suc = TextField()
    pl2_suc = TextField()
    processor = TextField()
    rmse_val_m = TextField()
    sensor = TextField()
    uid = PrimaryKeyField()
    val_ref_pt = TextField()
    x_shift_m = TextField()
    y_shift_m = TextField()
    z_shift_m = TextField()

    class Meta:
        db_table = 'cephgeo_lidarcoverageblock'


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
    demid = PrimaryKeyField()
    name = CharField()
    type = CharField()
    dem_file_path = TextField(null=False)
    cephdataobject = ForeignKeyField(db_column='cephdataobject_id', rel_model=CephDataObject, to_field='id')
    lidar_block = ForeignKeyField(db_column='lidar_block_id', rel_model=Cephgeo_LidarCoverageBlock, to_field='uid')
    
    class Meta:
        db_table = 'automation_demdatastore'

from django.utils.encoding import smart_str
from models import PSQL_DB, BaseModel, DataClassification, \
    Automation_AutomationJob, Cephgeo_LidarCoverageBlock, Cephgeo_CephDataObject
import os
import logging

logger = logging.getLogger()


def get_data_class_from_filename(filename):
    data_classification = DataClassification.labels[DataClassification.UNKNOWN]

    for x in DataClassification.filename_suffixes:
        filename_patterns = x.split(".")
        if filename_patterns[0] in filename.lower() and filename_patterns[1] \
                in filename.lower():
            data_classification = DataClassification.filename_suffixes[x]

    return data_classification


def get_uid_from_filename(filename):
    name = filename.split('.')[0]
    uid = int((name.split('_')[3].split('U')[-1:]).pop(0))
    print 'UID', uid

    return uid


def create_gridref_dict(ceph_obj, gridref_dict_by_data_class):
    if DataClassification.gs_feature_labels[ceph_obj.data_class] in \
            gridref_dict_by_data_class:
        gridref_dict_by_data_class[DataClassification.gs_feature_labels[
            ceph_obj.data_class].encode('utf8')] \
            .append(ceph_obj.grid_ref.encode('utf8'))
    else:
        gridref_dict_by_data_class[DataClassification.gs_feature_labels[
            ceph_obj.data_class].encode('utf8')] = \
            [ceph_obj.grid_ref.encode('utf8'), ]


def transfer_metadata(log_file):
    print 'Uploading metadata to LiPAD...'
    print 'Reading %s ...' % log_file




def ceph_metadata_update(uploaded_objects_list, update_grid=True):
    #: transform ceph_upload_log to list

    # Pop first line containing header
    uploaded_objects_list.pop(0)
    """NAME,LAST_MODIFIED,SIZE_IN_BYTES,CONTENT_TYPE,GEO_TYPE,FILE_HASH GRID_REF"""

    # Loop through each metadata element
    csv_delimiter = ','
    objects_inserted = 0
    objects_updated = 0
    gridref_dict_by_data_class = dict()

    logger.info("Encoding {0} ceph data objects".format(
        len(uploaded_objects_list)))

    for ceph_obj_metadata in uploaded_objects_list:
        metadata_list = ceph_obj_metadata.split(csv_delimiter)
        # logger.info("-> {0}".format(ceph_obj_metadata))
        # Check if metadata list is valid
        if len(metadata_list) is 6:
            # try:
            """
                Retrieve and check if metadata is present, update instead if there is
            """
            ceph_obj = None
            # for x in metadata_list:
            #     print 'Metadata', x
            try:
                ceph_obj = Cephgeo_CephDataObject.get(name=metadata_list[0])
                # Commented attributes are not relevant to update
                # ceph_obj.grid_ref = metadata_list[5]
                # ceph_obj.data_class = get_data_class_from_filename(metadata_list[0])
                # ceph_obj.content_type = metadata_list[3]

                # ceph_obj.last_modified = metadata_list[1]
                # ceph_obj.size_in_bytes = metadata_list[2]
                # ceph_obj.file_hash = metadata_list[4]

                # ceph_obj.save()

                with PSQL_DB.atomic() as txn:
                    new_q = (Cephgeo_CephDataObject
                             .update(last_modified=metadata_list[1],
                                     size_in_bytes=metadata_list[2],
                                     file_hash=metadata_list[4])
                             .where(Automation_AutomationJob.name == metadata_list[0]))
                    new_q.execute()

                objects_updated += 1

            except Cephgeo_CephDataObject.DoesNotExist:
                # ceph_obj = Cephgeo_CephDataObject(name=metadata_list[0],
                #                                   last_modified=metadata_list[1],
                #                                   size_in_bytes=metadata_list[2],
                #                                   content_type=metadata_list[3],
                #                                   data_class=get_data_class_from_filename(
                #     metadata_list[0]),
                #     file_hash=metadata_list[4],
                #     grid_ref=metadata_list[5],
                #     block_uid=get_uid_from_filename(metadata_list[0]))

                with PSQL_DB.atomic() as txn:
                    new_q = (Cephgeo_CephDataObject
                             .create(name=metadata_list[0],
                                     last_modified=metadata_list[1],
                                     size_in_bytes=metadata_list[2],
                                     content_type=metadata_list[3],
                                     data_class=get_data_class_from_filename(
                                 metadata_list[0]),
                                 file_hash=metadata_list[4],
                                 grid_ref=metadata_list[5],
                                 block_uid=get_uid_from_filename(
                                 metadata_list[0])))
                # ceph_obj.save()

                objects_inserted += 1

            if ceph_obj is not None:
                # Construct dict of gridrefs to update
                if DataClassification.gs_feature_labels[ceph_obj.data_class] in \
                        gridref_dict_by_data_class:
                    gridref_dict_by_data_class[DataClassification.gs_feature_labels
                                               [ceph_obj.data_class].encode('utf8')].append(ceph_obj.grid_ref.encode('utf8'))
                else:
                    gridref_dict_by_data_class[DataClassification.gs_feature_labels[
                        ceph_obj.data_class].encode('utf8')] = [ceph_obj.grid_ref.encode('utf8'), ]
            # except Exception as e:
            #    print("Skipping invalid metadata list: {0}".format(metadata_list))
        else:
            print("Skipping invalid metadata list (invalid length): {0}".format(
                metadata_list))

    # Pass to celery the task of updating the gird shapefile
    result_msg = "Succesfully encoded metadata of [{0}] of objects. Inserted [{1}], updated [{2}].".format(
        objects_inserted + objects_updated, objects_inserted, objects_updated)

    # if update_grid:
    #     result_msg += " Starting feature updates for PhilGrid shapefile."
    #     grid_feature_update.delay(gridref_dict_by_data_class)
    # print result_msg


def transform_log_to_list(log):
    log_list = []
    log = log.strip()
    log_list = log.split('\r\n')

    # print 'Log List:', log_list
    logging.info('Cleaned log.')

    return log_list


def upload_metadata(job):
    uploaded_objects_list = transform_log_to_list(job.ceph_upload_log)
    ceph_metadata_update(uploaded_objects_list)

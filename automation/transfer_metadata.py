from django.utils.encoding import smart_str
import os


def transfer_metadata(log_file):
    print 'Uploading metadata to LiPAD...'
    print 'Reading %s ...' % log_file


def parse_ceph_log(file_path, data_class):
    if not os.path.isfile(os.path.abspath(file_path)):
        # if os.path.isfile(file_path):
        print '{0} file not found'.format(file_path)
    else:
        csv_delimeter = ','
        with open(file_path, 'r') as open_file:
            of = open_file.read()
            first_line = True
            for line in of.split('\n'):
                tokens = line.strip().split(csv_delimeter)

                if first_line:
                    first_line = False
                    continue
                if line:
                    try:
                        if len(tokens) == 6:
                            print 'Tokens'
                            print tokens[0]
                            print tokens[1]
                            print tokens[2]
                            print tokens[3]
                            print data_class
                            print tokens[4]
                            print tokens[5]
                            print 'Creating Ceph Data Object...'
                            try:
                                # Cephgeo_CephDataObject.create(name=tokens[0],
                                #                               last_modified=tokens[
                                #                                   1],
                                #                               size_in_bytes=tokens[
                                #                                   2],
                                #                               content_type=tokens[
                                #                                   3],
                                #                               data_class=data_class,
                                #                               file_hash=tokens[4],
                                #                               grid_ref=tokens[5])
                                ceph_obj = 'Ceph object'
                                if ceph_obj is not None:
                                    #  if DataClassification.gs_feature_labels[ceph_obj.data_class] in gridref_dict_by_data_class:
                                    #     gridref_dict_by_data_class[DataClassification.gs_feature_labels[
                                    #         ceph_obj.data_class].encode('utf8')].append(ceph_obj.grid_ref.encode('utf8'))
                                    # else:
                                    #     gridref_dict_by_data_class[DataClassification.gs_feature_labels[
                                    # ceph_obj.data_class].encode('utf8')] =
                                    # [ceph_obj.grid_ref.encode('utf8'), ]
                                    pass
                            except Exception:
                                print 'Could not create ceph data object'
                        else:
                            print("Skipping invalid metadata list (invalid length): {0}".format(
                                metadata_list))
                    except Exception:
                        print 'Error in updating grid?'

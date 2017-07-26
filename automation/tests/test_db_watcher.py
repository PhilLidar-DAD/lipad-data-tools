#!/usr/bin/env python
import sys
import os
sys.path.append(os.path.dirname(
    os.path.realpath(__file__)).split('automation')[0])
# from automation.models import *
# from workers import *
# from transfer_metadata import *

# file_path = 'dump/uploaded_objects_[Agno_Blk5C]_2017-05-09-1450-27.txt'
# data_class = 'LAZ'

# parse_ceph_log(file_path, data_class)
# gridref_dict_by_data_class = dict()
# if DataClassification.gs_feature_labels[3] in gridref_dict_by_data_class:
#     print 'YES'
# else:
#     print 'NO'
#
#
# print Automation_AutomationJob.STATUS_CHOICES
print os.path.dirname(os.path.realpath(__file__))
# db_watcher()

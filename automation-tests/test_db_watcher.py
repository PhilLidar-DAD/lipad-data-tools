#!/usr/bin/env python
import sys
import os
from os.path import dirname, abspath
sys.path.append(dirname(dirname(abspath(__file__))))
# sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from automation.workers import *
from automation.transfer_metadata import *

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

db_watcher()

#!/usr/bin/env python

import argparse
import logging
import os
import os.path
import subprocess
import sys
import re
from pprint import pprint


_version = '0.0.1'
print os.path.basename(__file__) + ': v' + _version
_logger = logging.getLogger(__name__)
_LOG_LEVEL = logging.DEBUG
CAMCTRL = ['camcontrol','devlist']
SMARTCK = ['smartctl', '-A']
DISK_NUM= 203
CRITICAL_SMART_VALS = ['5','10','183','184','187','188','196','197','198','201']

def _setup_logging(args=None):
    # Setup logging
    _logger.setLevel(_LOG_LEVEL)
    formatter = logging.Formatter('%(message)s')

    # Check verbosity for console
    if args and args.verbose >= 1:
        _CONS_LOG_LEVEL = logging.DEBUG
    elif args:
        _CONS_LOG_LEVEL = logging.INFO
    else:
        _CONS_LOG_LEVEL = logging.WARN

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

def _parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version',
                        version=_version)
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-d', '--disknum', default=0)
    args = parser.parse_args()
    return args


def list_disks():
    list_cmd = subprocess.check_output(CAMCTRL)
    hdd_list = []
    for device in list_cmd.split('\n'):
        #dev_name = device.split(',')[1]
        dev_name = device[device.find("(")+1:device.find(")")].split(',')
        #print str(dev_name)
        if len(dev_name) !=2:
            pass
        elif dev_name[0].startswith('da'):
            hdd_list.append(dev_name[0])
        elif dev_name[1].startswith('da'):
            hdd_list.append(dev_name[1])
        else:
            pass
    pprint(hdd_list)
    return hdd_list

def smart_check(hdd_name):
    cmd = SMARTCK+[hdd_name]
    output=""
    try:
        smart_lines = subprocess.check_output(cmd).split('\n')
        #print smart_lines
        for line in smart_lines[7:]:
            if len(line) > 0:
                tokens = line.split()
                # SMART ID @ [0], SMART VAL @ [-1]
                if tokens[0] in CRITICAL_SMART_VALS and int(tokens[-1]) > 0:
                    output += "{0},{1},{2},".format(tokens[0],tokens[1],tokens[-1])
    except Exception as e:
        output +="ERR_NO_SMART"
    return output

def get_hdd_serial(hdd_name):
    cmd = "camcontrol identify {0}".format(hdd_name)
    #regex filter
    r = re.compile('^serial')
    
    output=""
    try:
        camctrl_out = subprocess.check_output(cmd)
        output = filter(r.match, camctrl_out.split("\n"))[0].split()[2]
    except Exception as e:
        output +="NO_SERIAL"
    return output
    
if __name__ == '__main__':

    # Parse arguments
    args = _parse_arguments()

    # Setup logging
    _setup_logging(args)
    
    max_disk = int(args.disknum)
    
    for hdd_num in xrange(max_disk+1):
        hdd_name = "/dev/da"+str(hdd_num)
        smart_stats = smart_check(hdd_name)
        serial_num = get_hdd_serial(hdd_name)
        if smart_stats:
            _logger.info(serial_num+","+hdd_name+","+smart_stats)

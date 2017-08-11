import json
import os
import logging
import subprocess
from pprint import pformat
import re


logger = logging.getLogger()
LOG_LEVEL = logging.INFO

def verify_las(file_path, checksum):

    outfile = file_path + '.lasinfo'
    output = None
    # Check if json output file exists
    if os.path.isfile(outfile):
        try:
            # Load output from json file
            output = json.load(open(outfile, 'r'))

            # Reverify if dll wasn't loaded last time
            if "can't load requested dll" in output['out']:
                output = None
        except Exception:
            pass

    if (output is None or
            (output and 'checksum' in output and
                output['checksum'] != checksum)):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['lasinfo', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        if "can't load requested dll" in output['out']:
            logger.error('Error loading requested dll! Exiting.')
            logger.error('out:\n%s', pformat(out))
            exit(1)
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True, ensure_ascii=False)

    # Save checksum in output if missing
    if 'checksum' not in output:
        output['checksum'] = checksum
        json.dump(output, open(outfile, 'w'), indent=4, sort_keys=True,
                  ensure_ascii=False)

    # Determine if file is corrupted from output
    remarks_buf = []
    has_error = False

    if output['returncode'] != 0:
        has_error = True
        remarks_buf.append('Error while opening file')

    # Ignore these warning messages
    ignored = [r'points outside of header bounding box',
               r'range violates gps week time specified by global encoding bit 0',
               r'for return [0-9]+ real number of points by return \([0-9]+\) is different from header entry \([0-9]+\)']
    # Parse output for warning messages
    for l in output['out'].split('\n'):
        line = l.strip()

        if 'error' in line:
            has_error = True
            remarks_buf.append(line)

        if 'warning' in line:
            remarks_buf.append(line)

            # Check if warning is ignored
            ignore_line = False
            for i in ignored:
                if re.search(i, line):
                    ignore_line = True
                    break
            if not ignore_line:
                has_error = True

    remarks = '\n'.join(remarks_buf)

    return has_error, remarks

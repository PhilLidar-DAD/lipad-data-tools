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


def get_checksums(file_path, skip_checksum=False):

    dir_path, filename = os.path.split(file_path)

    # Check if SHA1SUMS file already exists
    checksum = None
    sha1sum_filepath = os.path.join(dir_path, 'SHA1SUMS')
    if os.path.isfile(sha1sum_filepath):
        # Read files from SHA1SUM file that already have checksums
        with open(sha1sum_filepath, 'r') as open_file:
            for line in open_file:
                tokens = line.strip().split()
                # Strip wildcard from filename if it exists
                fn = tokens[1]
                if fn.startswith('?'):
                    fn = fn[1:]
                if fn == filename:
                    checksum = tokens[0]
    if not checksum and not skip_checksum:
        # Compute checksum
        shasum = subprocess.check_output(['sha1sum', file_path])
        tokens = shasum.strip().split()
        checksum = tokens[0][1:]

    # Check if LAST_MODIFIED file already exists
    last_modified = None
    last_modified_filepath = os.path.join(dir_path, 'LAST_MODIFIED')
    if os.path.isfile(last_modified_filepath):

        last_modified_all = json.load(open(last_modified_filepath, 'r'))
        if filename in last_modified_all:
            last_modified = last_modified_all[filename]
    if not last_modified:
        # Get last modified time
        last_modified = os.stat(file_path).st_mtime

    return checksum, int(last_modified)


def ignore_file_dir(name):
    # Hidden files/dirs
    if name.startswith('.'):
        return True
    # Checksums
    if name in ['LAST_MODIFIED', 'SHA1SUMS']:
        return True
    # Output files
    for file_ext in ['.gdalinfo', '.ogrinfo', '.lasinfo', '.7za']:
        if name.endswith(file_ext):
            return True

    return False


# def verify_file(file_path):
#     # Get checksums and last modified time
#     checksum, last_modified = get_checksums(file_path)

#     has_error, remarks = verify_las(file_path, checksum)


def verify_dir(dir_path):
    # Get file list
    file_list = {}
    for f in sorted(os.listdir(dir_path)):
        # Ignore some files/dirs
        if ignore_file_dir(f):
            continue
        fp = os.path.join(dir_path, f)
        if os.path.isfile(fp):
            file_list[fp] = None
    logger.debug('file_list:\n%s', pformat(file_list, width=40))

    # Verify files
    for fp in file_list.viewkeys():
        # file_list[fp] = verify_file(fp)
        # Get checksums and last modified time
        checksum, last_modified = get_checksums(fp)

        has_error, remarks = verify_las(fp, checksum)

        if has_error:
            return True

    return False

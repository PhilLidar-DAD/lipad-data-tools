def find_in_coverage(block_name):
    """
        Lidar coverage block in LiPAD DB
        Assume an active connection is present
    """
    try:
        block = Cephgeo_LidarCoverageBlock.get(block_name=block_name)
        uid = block.uid
        print 'Block in Lidar Coverage'
        print 'Block UID:', uid
        return True, uid
    except Exception:
        print 'Block not in Lidar Coverage', block_name
        return False, 0


def proper_block_name(block_path):

    # input format: ../../Agno_Blk5C_20130418

    # parses blockname from path
    block_name = block_path.split(os.sep)[-1]
    if block_name == '':
        block_name = block_path.split(os.sep)[-2]
    # remove date flown
    block_name = block_name.rsplit('_', 1)[0]

    return block_name

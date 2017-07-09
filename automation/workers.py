from automation_exceptions import *

from transfer_metadata import *
from utils import *
from exceptions import *
from datetime import datetime
import os, sys, logging, time, csv
import psycopg2
import settings
from shapely.geometry.geo import shape
from exceptions import *
from exceptions import *
import ast

#add georefmapper location to python path for import
georefmapper_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)).split('automation')[0],"postprocess")
print "Importing georefmapper from ", georefmapper_dir 
sys.path.append(georefmapper_dir)
import georefmapper.mapper

logger = logging.getLogger()
LOG_LEVEL = logging.DEBUG
CONS_LOG_LEVEL = logging.DEBUG
FILE_LOG_LEVEL = logging.DEBUG


def setup_logging():

    # Setup logging
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d)\t: %(message)s')

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Setup file logging
    LOG_FILE = os.path.splitext(__file__)[0] + '.log'
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel(FILE_LOG_LEVEL)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def connect_db():
    retry = True
    while retry:
        try:
            PSQL_DB.connect()
            print 'Connected to DB...'
            retry = False
            print 'Retry', retry
        except Exception:
            delay = random.randint(0, 30)
            logger.exception(
                'Error connecting to database. Retrying in %ss...', delay)
            retry = True
            time.sleep(delay)


def close_db():
    if not PSQL_DB.is_closed():
        PSQL_DB.close()


def process_job(q):
    assign_status(q, 1)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    print 'Processing Job'
    datatype = q.datatype
    input_dir = q.input_dir
    output_dir = q.output_dir
    processor = q.processor

    block_name = proper_block_name(input_dir)
    output_dir = q.output_dir.__add__('/' + block_name)

    print 'BLOCK NAME', block_name

    in_coverage, block_uid = find_in_coverage(block_name)

    # check first if Block Name is in Lidar Coverage
    if in_coverage:
        print 'Found in Lidar Coverage model', block_name, block_uid
        if datatype.lower() == ('laz' or 'ortho'):
            if not files_renamed(input_dir):
                rename_tiles.rename_tiles(input_dir, output_dir, processor, block_uid)
        else:
            raise Exception(
                'Handler not implemented for type: ' + str(q.datatype))

        assign_status(q, 2)
        print 'Status', q.status
        print 'Status Timestamp', q.status_timestamp
        ceph_uploaded, log_file = ceph_upload(output_dir)

        if ceph_uploaded:
            assign_status(q, 3)
            transfer_metadata(log_file, datatype)

    else:
        print 'ERROR NOT FOUND IN MODEL', block_name, block_uid

def handle_dem(q):
    assign_status(q, 1)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    print 'Processing Job'
    
    # Convert to ASCII and assign variables
    input_dir = q.input_dir
    
    # Check if input_dir is windows, convert if so
    if input_dir.contains("\\"):
        input_dir = convert_windows_dem_path(input_dir)
    output_dir = ast.literal_eval(q.output_dir)
    processor = convert_to_string(q.processor)
    dem_input_dict = ast.literal_eval(input_dir.replace("\'",""))
    dem_name = dem_input_dict["dem_name"]
    
    # Check if DEM dir is windows, convert if so
    dem_dir = dem_input_dict["dem_file_path"]
    if dem_dir.contains("\\"):
        dem_dir = convert_windows_dem_path(dem_dir)
    input_block_list = dem_input_dict["blocks"]
    
    print "DEM job input:"
    pprint(dem_input_dict)

    # Tile DEM
    tile_output_dir=""
    if q.datatype.upper() == DataClassification.gs_feature_labels[DataClassification.DTM]:
        """Tile DTM only"""
        status, tile_output_dir = tile_dtm(dem_dir, output_dir)
    elif q.datatype.upper() == DataClassification.gs_feature_labels[DataClassification.DSM]:
        """Tile DSM only"""
        status, tile_output_dir = tile_dsm(dem_dir, output_dir)
    else:
        raise NotImplementedException("Unhandled type in DEM worker: {0}".format(q.datatype))
    
    # Upload to Ceph
    assign_status(q, 2)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    ceph_uploaded, ceph_upload_log_file = ceph_upload(output_dir, os.path.join(tile_output_dir,"ceph_upload.dump"))

    # Map metadata to tiles in LiPAD
    if ceph_uploaded:
        assign_status(q, 3)
        lidar_coverage_block_metadata_dict = map_ceph_objects_to_lidar_block_metadata(input_block_list, ceph_upload_log_file, dem_name, convert_to_string(q.datatype.upper()), dem_dir)
    else:
        raise CephUploadFailedException("Upload to Ceph failed for job of type: "+q.datatype)

def make_psql_list(input_list):
    pprint(input_list)
    if len(input_list) > 1:
        return  str(tuple(input_list))
    else:
        return "({0})".format(input_list[0])

def fetch_lidar_blocks_from_postgis(block_uid_list):
    """
        Remotely accesses postgis and retrieves lidar block metadata from settings.LIDAR_COVERAGE_TABLE_NAME
        
        @param block_uid_list: list of block uids to retrieve from postgis
             
        @return queryset for  blocks in block_uid_list, containing attributes and the block's geometry in geojson
    """
    POSTGIS_QUERY = """
SELECT 
    "UID", "Block_Name", "Adjusted_L", "Sensor", "Base_Used", "Processor",
    "Flight_Num", "Mission_Na", "Date_Flown", "X_Shift_m", "Y_Shift_m",
    "Z_Shift_m", "Height_dif", "RMSE_Val_m", "Cal_Ref_Pt", "Val_Ref_Pt",
    "Floodplain", "PL1_SUC", "PL2_SUC", "Area_sqkm", ST_AsGeoJSON(the_geom) 
FROM {0}
WHERE
    "UID" IN {1};""".format(settings.LIDAR_COVERAGE_TABLE_NAME, make_psql_list(block_uid_list))
    conn = psycopg2.connect(("host={0} dbname={1} user={2} password={3}".format
                             (settings.DB_HOST, settings.GIS_DB_NAME,
                              settings.DB_USER, settings.DB_PASS)))
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dict_cur.execute(POSTGIS_QUERY)
    # result is a queryset of Python dictionaries
    return dict_cur.fetchall()

def create_georef_ceph_object_map(ceph_upload_log_file):
    """
        Remotely accesses postgis and retrieves lidar block metadata from settings.LIDAR_COVERAGE_TABLE_NAME
        
        @param ceph_upload_log_file: output file of Ceph upload script
             
        @return dictionary mapping each georef to the Ceph objects of that georef
    """
    
    georef_dict = dict()
    
    with open(ceph_upload_log_file, 'rb') as f:
        reader = csv.reader(f)
        next(reader)    # Skip first line, header of CSV file
        for row in reader:
            if len(row) == 6:
                if row[5] in georef_dict:
                    georef_dict[row[5]].append(row)
                else:
                    georef_dict[row[5]] = [row]
    
    return georef_dict        

def map_ceph_objects_to_lidar_block_metadata(input_block_list, ceph_upload_log_file, dem_name, dem_type, dem_path):
    """
        Returns a dictionary of dictionaries, each dictionary representing 
        a lidar coverage block with the following mapping:
        
        UID : lidar_block_metadata_dict
        
        lidar_block_metadata_dict contents:
            UID            - uid of the block
            Block_Name     - name of the block
            Adjusted_L     -
            Sensor         - sensor used for data acquisition
            Base_Used
            Processor
            Flight_Num
            Mission_Na
            Date_Flown
            X_Shift_m
            Y_Shift_m
            Z_Shift_m
            Height_dif
            RMSE_Val_m
            Cal_Ref_Pt
            Val_Ref_Pt
            Floodplain
            PL1_SUC
            PL2_SUC
            Area_sqkm
            Shapely_Geom    - Shapely geometry for use with georefmapper
            Georefs         - List of georefs intersected by this block
    """
    
    # Convert block_names to block_uids, 
    # for encoding later, store the list as in a dict with UIDs as keys
    block_uid_list = []
    uid_to_block_input_data_dict = dict()
    for block_tokens in input_block_list:
        block_name=block_tokens[0]
        in_coverage, block_uid, block_name = find_in_coverage(block_name)
        block_uid_list.append(block_uid)
        uid_to_block_input_data_dict[block_uid]=block_tokens
    
    
    lidar_coverage_block_metadata_dict = dict()
    time_total = 0.0
    count = 0
    blocks_from_postgis = fetch_lidar_blocks_from_postgis(block_uid_list)
    
    georef_to_ceph_objects_dict = create_georef_ceph_object_map(ceph_upload_log_file)
    
    for lidar_coverage_block_db_data in blocks_from_postgis:
        start_time = time.time()
        
        # parse attribute table into a new dictionary, disregard geometry 
        dict_keys = list(lidar_coverage_block_db_data.keys())
        dict_keys.remove('st_asgeojson')
        block_metadata_dict = {k: lidar_coverage_block_db_data[k] for k in dict_keys}
        
        # convert block geometry into shapely geometry format
        block_metadata_dict["Shapely_Geom"] = shape(json.loads(lidar_coverage_block_db_data['st_asgeojson']))
        block_metadata_dict["Georefs"] = georefmapper.mapper.georefs_in_geom(block_metadata_dict["Shapely_Geom"])
        
        lidar_coverage_block_metadata_dict[lidar_coverage_block_db_data["UID"]] = block_metadata_dict
        
        #Create DEM data store object for this DEM upload
        demdatastore_obj = Automation_Demdatastore.create( name=dem_name,
                                                                        type=dem_type,
                                                                        dem_file_path=dem_path,)

        
        #Loop through georefs
        skipped_georefs = 0
        for georef in block_metadata_dict["Georefs"]:
            try:
                ceph_object_metas = georef_to_ceph_objects_dict[georef]
                
                #Loop through all ceph object metadata with said georef
                for com in ceph_object_metas:
                    print "processing meta for: ", str(com)
                    #Create CephDataObject instance
                    ceph_obj = None
                    ceph_objects_inserted = 0
                    ceph_objects_updated = 0
                    ceph_obj, cdo_created = CephDataObject.get_or_create(name=com[0],
                                                                        last_modified=com[1],
                                                                        size_in_bytes=com[2],
                                                                        content_type=com[3],
                                                                        data_class=get_data_class_from_filename(
                                                                            com[0]),
                                                                        file_hash=com[4],
                                                                        grid_ref=com[5])
                    ceph_obj.save()
                    if cdo_created:
                        ceph_objects_inserted += 1
                    else:
                        ceph_objects_updated += 1
                    lidar_block_obj = Cephgeo_LidarCoverageBlock.get(uid=lidar_coverage_block_db_data["UID"])
                    
                    #Create mapping instance between DEM Data Store, Ceph Object, and Lidar Coverage Block instances
                    #Added shifting vals, height diff, and rmse
                    #block input list = [block_name_a,shifting_val_x,shifting_val_y,shifting_val_z,height_diff,rmse]
                    block_input_list = uid_to_block_input_data_dict[lidar_coverage_block_db_data["UID"]]
                    dcom_obj = Automation_Demcephobjectmap.create(  cephdataobject=ceph_obj,
                                                                    demdatastore=demdatastore_obj,
                                                                    lidar_block=lidar_block_obj,
                                                                    shifting_val_x = float(block_input_list[1]),
                                                                    shifting_val_y = float(block_input_list[2]),
                                                                    shifting_val_z = float(block_input_list[3]),
                                                                    height_diff = float(block_input_list[4]),
                                                                    rmse = float(block_input_list[5]))
                    dcom_obj.save()

            except KeyError:
                print "WARNING: No Ceph Object found with georef [{0}] from lidar block [{1}]. Skipping...".format(georef, block_metadata_dict["Block_Name"])
                skipped_georefs += 1 
            
        # Print stats
        print "Total skipped georefs: " + str(skipped_georefs)
        end_time = time.time()
        elapsed_time=round(end_time - start_time,2)
        time_total += elapsed_time
        count += 1

    print "Average time for {0} blocks: {1}".format(count, time_total/float(count))
    
    return lidar_coverage_block_metadata_dict

def transfer_dem_metadata(lidar_coverage_block_metadata_dict, ceph_log_file_path):
    """
    @todo: 
    1. Create dictionary of gridref to ceph objects
    2. Loop through lidar block metadata
        For each block metadata: 
        a. Loop through georefs:
            For each georef
            a. get corresponding Ceph object metadata, and get_or_create Automation_CephDataObject instances
            b. create Automation_DemDataStore instance, map to corresponding:
                - CephgGeo_LidarBlockMetadata [UID]
                - Automation_CephDataObject [id]
        b. 
        
    """

def db_watcher():
    """
        Watch LiPAD DB Automation Table for pending jobs
    """
    print 'Starting...'
    setup_logging()
    while True:
        connect_db()

        for status in Automation_AutomationJob.STATUS_CHOICES:
            try:
                q = Automation_AutomationJob.get(status=status)
                print 'Query found!'
                print q.status
                # if s.__eq__('pending_process'):
                #     if q.target_os.lower() == 'linux':
                #         process_job(q)
                #         # elif q.datatype.lower() == 'dtm':
                #     else:
                #         print 'PASS TO WINDOWS'
                #         # Windows poller
                # elif s.__eq__('done_ceph'):
                #     # in case upload from ceph to lipad was interrupted
                #     assign_status(q, 3)
                #     transfer_metadata()

            except Automation_AutomationJob.DoesNotExist:
                logger.error('No %s task', status)

            except Exception:
                logger.exception('Database watcher error!')
            finally:
                close_db()

        delay = get_delay(1, 10)
        logger.info('Worker Sleeping for %ssecs...', delay)
        time.sleep(delay)

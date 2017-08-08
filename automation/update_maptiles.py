from xml.etree import ElementTree

from settings import *

import logging
import requests
from datetime import datetime
from xml.etree import ElementTree

logger = logging.getLogger()

# Register namespaces
for namespace, url in NAMESPACES.items():
    ElementTree.register_namespace(namespace, url)


def send_request(query, method="GET", data=None):

    # Construct request
    logger.debug("URL: %s", GEOSERVER_URL + query)
    start_time = datetime.now()
    logger.debug("start_time = %s", start_time)

    # Check method
    if method == "POST":

        # Open request
        r = requests.post(GEOSERVER_URL + query,
                          auth=(GEOSERVER_USER, GEOSERVER_PASSWD),
                          data=data,
                          headers={'content-type': 'application/xml'})

    elif method == "GET":

        # Open request
        r = requests.get(GEOSERVER_URL + query,
                         auth=(GEOSERVER_USER, GEOSERVER_PASSWD))

    end_time = datetime.now()
    logger.debug("end_time = %s", end_time)
    logger.debug("duration = %s", end_time - start_time)

    # Check HTTP return status
    if r.status_code == 200:

        # Return content on successful status
        return r.text

    else:

        # Raise Exception for other return codes
        raise Exception("r.status_code =" + r.status_code)

def create_nested_gridref_filter(gridref_list):
    ogc_filter = """       <ogc:Filter>
         <Or>"""
    for gridref in gridref_list:
        ogc_filter += """
           <ogc:PropertyIsEqualTo>
             <ogc:PropertyName>GRIDREF</ogc:PropertyName>
             <ogc:Literal>{0}</ogc:Literal>
           </ogc:PropertyIsEqualTo>""".format(gridref)
    ogc_filter += """
         </Or>
       </ogc:Filter>"""
    return ogc_filter


def nested_grid_update(gridref_list, field_id, field_value, shapefile_name=GRID_SHAPEFILE, property_name="GRIDREF", ):
    xml_input = """<wfs:Transaction service="WFS" version="1.0.0"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:wfs="http://www.opengis.net/wfs">
  <wfs:Update typeName="{0}">
    <wfs:Property>
      <wfs:Name>{1}</wfs:Name>
      <wfs:Value>{2}</wfs:Value>
    </wfs:Property>
{3}
  </wfs:Update>
</wfs:Transaction>""".format(   shapefile_name,
                                field_id,
                                field_value,
                                create_nested_gridref_filter(gridref_list))
    xml_result = send_request("ows", "POST", xml_input)

    if "SUCCESS" in xml_result:
        return True
    else:
        return xml_result

def grid_feature_update(gridref_dict_by_data_class, field_value=1):
    """
        :param gridref_dict_by_data_class: contains mapping of [feature_attr] to [grid_ref_list]
        :param field_value: [1] or [0]
        Update the grid shapefile feature attribute specified by [feature_attr] on gridrefs in [gridref_list]
    """
    grid_updated = False
    # for feature_attr, grid_ref_list in gridref_dict_by_data_class.iteritems():
    #     logger.info("Updating feature attribute [{0}]".format(feature_attr))
    #     grid_updated = nested_grid_update(grid_ref_list, feature_attr, field_value)
    #     logger.info("Finished task for feature [{0}]".format(feature_attr))

    if grid_updated:
        return True
    else:
        return False


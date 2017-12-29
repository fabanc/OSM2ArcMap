import os, time, xml.etree.ElementTree as etree, bz2
import arcpy, numpy



# TODO Clean up memory consumed by reading xml elements

arcpy.env.overwriteOutput = True

STANDARD_FIELDS = set(('highway','name','name_en','ref','lanes','surface','oneway','maxspeed','tracktype','access','service','foot','bicycle','bridge','barrier','lit','layer',
                      'building','building_levels','building_height','addr_housenumber','addr_street','addr_city','addr_postcode','addr_country','addr_place','addr_state',
                      'natural','landuse','waterway','power','amenity','place','height','note','railway','public_transport','operator','guage','width','tunnel',
                    'leisure','is_in','ele','shop','man_made','parking',
                    'boundary','aerialway','aeroway','craft','emergency','geological','historic','military','office',
                    'sport','tourism','traffic_calming','entrance','crossing'))

STANDARD_FIELDS_ARRAY = list(STANDARD_FIELDS)
RELATION_FIELD_ARRAY = ['type'] + STANDARD_FIELDS_ARRAY

# arcgis_row_definition = namedtuple('ArcGISRow', ['type', 'Length', ])

NODE_ID_FIELD = arcpy.Field()
NODE_ID_FIELD.name = 'id'
NODE_ID_FIELD.type = 'String'
NODE_ID_FIELD.length = '30'

NODE_LON_FIELD = arcpy.Field()
NODE_LON_FIELD.name = 'lon'
NODE_LON_FIELD.type = 'Double'

NODE_LAT_FIELD = arcpy.Field()
NODE_LAT_FIELD.name = 'lat'
NODE_LAT_FIELD.type = 'Double'

TIMESTAMP_FIELD = arcpy.Field()
TIMESTAMP_FIELD.name = 'timestamp'
TIMESTAMP_FIELD.type = 'String'
TIMESTAMP_FIELD.length = 20

NODE_SAVED_ATTRIBUTES = [
    NODE_ID_FIELD,
    NODE_LON_FIELD,
    NODE_LAT_FIELD,
    TIMESTAMP_FIELD
]

WAY_SAVED_ATTRIBUTES = [
    NODE_ID_FIELD,
    TIMESTAMP_FIELD
]

TEMPORARY_LINES = 'in_memory/temp_way_nodes_lines'
COORDINATES_SYSTEM = arcpy.SpatialReference(4326)

def timeit(method):
    """
    Timing function. Used a decorator to measure the time taken by each function.
    :param method:
    :return:
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        arcpy.AddMessage('Method {} executed in {} seconds'.format(method.__name__, te - ts))
        return result

    return timed


def get_fields_numpy_definition(field_list):
    standard_fields_array_tuple = [('_ID', numpy.int)]
    for f in field_list:
        standard_fields_array_tuple.append((f, '|S255'))

    return numpy.array(
        [],
        numpy.dtype(standard_fields_array_tuple)
    )

def create_output_workspace(workspace):
    if not arcpy.Exists(workspace):
        workspace = arcpy.CreateFileGDB_management(os.path.split(workspace)[0], os.path.split(workspace)[1])


###################################
# PARSING FUNCTIONS FOR NODES, WAYS, AND RELATIONSHIPS
###################################
def parse_node_children(elem):
    tag_dict = {}
    for child in elem:
        if child.tag == 'tag':
            tag_dict[child.attrib['k']] = child.attrib['v']
    return tag_dict

def parse_way_children(elem):
    tag_dict = {}
    nodes = []
    for child in elem:
        if child.tag == 'tag':
            tag_dict[child.attrib['k']] = child.attrib['v']
        elif child.tag == 'nd':
            nodes.append(child.attrib['ref'])
    return tag_dict, nodes

def parse_relation_children(elem):
    tag_dict = {}
    members = []
    for child in elem:
        if child.tag == 'tag':
            tag_dict[child.attrib['k']] = child.attrib['v']
        elif child.tag == 'member':
            members.append(child)
    return tag_dict, members

###################################
# FUNCTIONS TO CREATE FEATURE CLASS
###################################
@timeit
def create_node_feature_class(workspace, feature_class_name, standard_fields):
    node_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "point", "#", "DISABLED", "DISABLED", COORDINATES_SYSTEM)
    for field in NODE_SAVED_ATTRIBUTES:
        arcpy.AddField_management(node_feature_class, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(node_feature_class, "OID@", standard_fields, "_ID")
    return node_feature_class


@timeit
def create_way_feature_class(workspace, table_name, standard_fields):
    way_tag_table = os.path.join(workspace, table_name)
    arcpy.CreateFeatureclass_management(workspace, table_name, "POLYLINE", "#", "DISABLED", "DISABLED",
                                        COORDINATES_SYSTEM)
    for field in WAY_SAVED_ATTRIBUTES:
        arcpy.AddField_management(way_tag_table, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(way_tag_table, "OID@", standard_fields, "_ID")
    return way_tag_table


@timeit
def create_way_nodes(workspace, table_name):
    way_nodes_table = os.path.join(workspace, table_name)
    arcpy.CreateTable_management(workspace, table_name)
    arcpy.AddField_management(way_nodes_table, 'way_id', NODE_ID_FIELD.type, "#", "#", NODE_ID_FIELD.length)
    arcpy.AddField_management(way_nodes_table, 'node_id', NODE_ID_FIELD.type, "#", "#", NODE_ID_FIELD.length)
    arcpy.AddField_management(way_nodes_table, 'sequence', NODE_ID_FIELD.type, "#", "#", NODE_ID_FIELD.length)
    return way_nodes_table

@timeit
def create_relations_table(workspace, table_name, standard_fields):
    relations_table = os.path.join(workspace, table_name)
    arcpy.CreateTable_management(workspace, table_name)
    for field in WAY_SAVED_ATTRIBUTES:
        arcpy.AddField_management(relations_table, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(relations_table, "OID@", standard_fields, "_ID")
    return relations_table


@timeit
def create_relations_member(workspace, table_name):
    relations_table = os.path.join(workspace, table_name)
    arcpy.CreateTable_management(workspace, table_name)
    arcpy.AddField_management(relations_table, NODE_ID_FIELD.name, NODE_ID_FIELD.type, "#", "#", NODE_ID_FIELD.length)
    arcpy.AddField_management(relations_table, 'type', 'STRING', "#", "#", 30)
    arcpy.AddField_management(relations_table, 'ref', NODE_ID_FIELD.type, "#", "#", NODE_ID_FIELD.length)
    arcpy.AddField_management(relations_table, 'role', 'STRING', "#", "#", 30)
    return relations_table


###################################
# PARSING FUNCTION
###################################
@timeit
def import_osm(osm_file, output_geodatabase, nodes_feature_class, way_feature_class, way_nodes_table_lines, relations_table, relations_members):
    node_base_attr = [field.name for field in NODE_SAVED_ATTRIBUTES]
    node_all_attr = ['SHAPE@XY'] + node_base_attr + STANDARD_FIELDS_ARRAY

    way_base_attr = [field.name for field in WAY_SAVED_ATTRIBUTES]
    way_tags_all_attr = way_base_attr + STANDARD_FIELDS_ARRAY

    relations_base_attr = [field.name for field in WAY_SAVED_ATTRIBUTES]
    relations_all_attr = relations_base_attr + RELATION_FIELD_ARRAY

    # Edit session is required to edit multiple feature class at a time within the same workspace
    with arcpy.da.Editor(output_geodatabase) as edit:
        with arcpy.da.InsertCursor(nodes_feature_class, node_all_attr) as insert_nodes_cursor:
            with arcpy.da.InsertCursor(way_feature_class, way_tags_all_attr) as insert_way_line_cursor:
                with arcpy.da.InsertCursor(way_nodes_table_lines, ['way_id', 'node_id', 'sequence']) as insert_way_nodes_lines_cursor:
                    with arcpy.da.InsertCursor(relations_table, relations_all_attr) as relations_table_cursor:
                        with arcpy.da.InsertCursor(relations_members, [NODE_ID_FIELD.name,'type','ref','role']) as relations_members_table_cursor:
                            for event, elem in etree.iterparse(osm_file):
                                if elem.tag == 'node':
                                    point_geom = [float(elem.attrib['lon']), float(elem.attrib['lat'])]
                                    tag_dict = parse_node_children(elem)
                                    attrib_values = [point_geom]
                                    for attr in node_base_attr:
                                        attrib_values.append(elem.attrib[attr])
                                    for key in STANDARD_FIELDS_ARRAY:
                                        if key in tag_dict:
                                            attrib_values.append(tag_dict[key])
                                        else:
                                            attrib_values.append(None)

                                    insert_nodes_cursor.insertRow((attrib_values))

                                if elem.tag == 'way':
                                    tag_dict, nodes = parse_way_children(elem)
                                    if len(nodes) >= 2:
                                        attrib_values = []
                                        for attr in way_base_attr:
                                            attrib_values.append(elem.attrib[attr])
                                        for key in STANDARD_FIELDS_ARRAY:
                                            if key in tag_dict:
                                                attrib_values.append(tag_dict[key])
                                            else:
                                                attrib_values.append(None)
                                        insert_way_line_cursor.insertRow((attrib_values))
                                        sequence = 1
                                        for node in nodes:
                                            insert_way_nodes_lines_cursor.insertRow(
                                                ([elem.attrib['id'], node, sequence]))
                                            sequence += 1
                                        # if nodes[0] == nodes[-1]:



                                    else:
                                        print 'Detected way with less than 2 nodes'

                                elif elem.tag == 'relation':
                                    tag_dict, members = parse_relation_children(elem)

                                    attrib_values = []
                                    for attr in relations_base_attr:
                                        attrib_values.append(elem.attrib[attr])

                                    for key in RELATION_FIELD_ARRAY:
                                        if key in tag_dict:
                                            attrib_values.append(tag_dict[key])
                                        else:
                                            attrib_values.append(None)

                                    relations_table_cursor.insertRow(attrib_values)

                                    for member in members:
                                        row = [
                                            elem.attrib[NODE_ID_FIELD.name],
                                            member.attrib['type'],
                                            member.attrib['ref']
                                        ]

                                        if 'role' in member.attrib:
                                            row.append(member.attrib['role'])
                                        else:
                                            row.append(None)

                                        relations_members_table_cursor.insertRow(row)


###################################
# FUNCTIONS TO BUILD LINES
###################################
@timeit
def build_ways(osm_ways_tables, nodes_feature_class):
    # Load nodes into memory
    nodes_tuples = []
    with arcpy.da.SearchCursor(nodes_feature_class, [NODE_ID_FIELD.name, 'SHAPE@X', 'SHAPE@Y']) as nodes_search_cursor:
        for row in nodes_search_cursor:
            nodes_tuples.append((row[0], row[1], row[2]))

    nodes_array = numpy.array(nodes_tuples,
                              numpy.dtype([
                                  ('_node_id', '|S255'),
                                  ('lon', numpy.float),
                                  ('lat', numpy.float)
                                ])

                              )
    for table in osm_ways_tables:
        arcpy.da.ExtendTable(
            table,
            'node_id',
            nodes_array,
            '_node_id'
        )

        # Make XY Event Layer
        points_layer = 'event_layer'
        arcpy.MakeXYEventLayer_management(table, 'lon', 'lat', points_layer, COORDINATES_SYSTEM)
        arcpy.PointsToLine_management(points_layer, TEMPORARY_LINES, Line_Field='way_id', Close_Line='NO_CLOSE')
        arcpy.Delete_management(points_layer)

@timeit
def build_lines(line_feature_class, line_temp_geometries):
    line_geom = {}
    with arcpy.da.SearchCursor(line_temp_geometries, ['way_id', 'SHAPE@WKT']) as search_cursor:
        for row in search_cursor:
            line_geom[row[0]] = row[1]

    with arcpy.da.Editor(output_geodatabase) as edit:
        with arcpy.da.UpdateCursor(line_feature_class, [NODE_ID_FIELD.name, 'SHAPE@WKT']) as update_cursor:
            for row in update_cursor:
                row[1] = line_geom[row[0]]
                update_cursor.updateRow(row)


###################################
# MAIN CALLING FUNCTION
###################################
def process(osm_file, output_geodatabase):
    create_output_workspace(output_geodatabase)

    additional_fields = get_fields_numpy_definition(STANDARD_FIELDS_ARRAY)
    additional_fields_relations = get_fields_numpy_definition(RELATION_FIELD_ARRAY)
    nodes_feature_class = create_node_feature_class(output_geodatabase, 'nodes', additional_fields)
    way_feature_class = create_way_feature_class(output_geodatabase, 'ways', additional_fields)
    way_nodes_table_lines = create_way_nodes(output_geodatabase, 'way_nodes')
    relations_table = create_relations_table(output_geodatabase, 'relations', additional_fields_relations)
    relations_members = create_relations_member(output_geodatabase, 'relations_members')

    # Parse the XML file
    import_osm(
            bz2.BZ2File(osm_file, 'r'),
            output_geodatabase,
            nodes_feature_class,
            way_feature_class,
            way_nodes_table_lines,
            relations_table,
            relations_members
    )

    arcpy.AddIndex_management(
        nodes_feature_class,
        [NODE_ID_FIELD.name],
        index_name='{}_idx'.format(NODE_ID_FIELD.name),
        unique=True)

    # Make lines and polygon geomtries from points
    way_nodes_tables = [way_nodes_table_lines]
    for table in way_nodes_tables:
        arcpy.AddIndex_management(table, ['node_id'],index_name='node_id_idx', unique='NON_UNIQUE')
        arcpy.AddIndex_management(table, ['way_id'], index_name='way_id_idx', unique='NON_UNIQUE')


    build_ways(way_nodes_tables, nodes_feature_class)

    build_lines(
        way_feature_class,
        TEMPORARY_LINES
    )
    arcpy.Delete_management(TEMPORARY_LINES)



if __name__ == '__main__':
    # osm_file = r'D:\Temp\Custom OSM Parser\monaco-latest.osm.bz2'
    # output_geodatabase = r'D:\Temp\Custom OSM Parser\monaco-latest.gdb'
    osm_file = arcpy.GetParameterAsText(0)
    output_geodatabase = arcpy.GetParameterAsText(1)
    process(osm_file, output_geodatabase)

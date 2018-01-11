# "D:\Temp\Custom OSM Parser\luxembourg-latest.osm.bz2" "D:\Temp\Custom OSM Parser\luxembourg-latest.gdb"
# "D:\Temp\Custom OSM Parser\monaco-latest.osm.bz2" "D:\Temp\Custom OSM Parser\monaco-latest.gdb"
import os, time, bz2, tempfile, time, csv, itertools

# import xml.etree.ElementTree as etree
from lxml import etree
import arcpy, numpy

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

# TEMPORARY_LINES = 'in_memory/temp_way_nodes_lines'
COORDINATES_SYSTEM = arcpy.SpatialReference(4326)

BOOLEAN_YES = 'YES'
BOOLEAN_NO = 'NO'

IDENTIFIER_DELIMITER = '|'
CSV_DELIMITER = ','
CSV_NODES = 'nodes.csv'
CSV_WAY_NODES = 'way_nodes.csv'


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
        child.clear()
    return tag_dict

def parse_way_children(elem):
    tag_dict = {}
    nodes = []
    for child in elem:
        if child.tag == 'tag':
            tag_dict[child.attrib['k']] = child.attrib['v']
        elif child.tag == 'nd':
            nodes.append(child.attrib['ref'])
        child.clear()
    return tag_dict, nodes

def parse_relation_children(elem):
    tag_dict = {}
    members = []
    type = None
    for child in elem:
        if child.tag == 'tag':
            tag_dict[child.attrib['k']] = child.attrib['v']
            if child.attrib['k'] == 'type':
                type = child.attrib['v']
        elif child.tag == 'member':
            members.append(child)

    return tag_dict, members, type

###################################
# FUNCTIONS TO CREATE FEATURE CLASS
###################################

# def create_relation_attributes(workspace, feature_class_name, standard_fields):
#     attribute_table = os.path.join(workspace, feature_class_name)
#     arcpy.CreateTable_management(workspace, table_name)
#     for field in WAY_SAVED_ATTRIBUTES:
#         arcpy.AddField_management(way_tag_table, field.name, field.type, "#", "#", field.length)
#     arcpy.da.ExtendTable(way_tag_table, "OID@", standard_fields, "_ID")

@timeit
def create_node_feature_class(workspace, feature_class_name, standard_fields):
    node_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "point", "#", "DISABLED", "DISABLED", COORDINATES_SYSTEM)
    for field in NODE_SAVED_ATTRIBUTES:
        arcpy.AddField_management(node_feature_class, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(node_feature_class, "OID@", standard_fields, "_ID")
    return node_feature_class


@timeit
def create_way_line_geom_feature_class(workspace, feature_class_name):
    way_tag_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "POLYLINE", "#", "DISABLED", "DISABLED",COORDINATES_SYSTEM)
    arcpy.AddField_management(way_tag_feature_class, NODE_ID_FIELD.name, 'STRING', "#", "#", NODE_ID_FIELD.length)
    return way_tag_feature_class


@timeit
def create_way_polygon_geom_feature_class(workspace, feature_class_name):
    way_tag_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "POLYGON", "#", "DISABLED", "DISABLED",COORDINATES_SYSTEM)
    arcpy.AddField_management(way_tag_feature_class, NODE_ID_FIELD.name, 'STRING', "#", "#", NODE_ID_FIELD.length)
    return way_tag_feature_class

@timeit
def create_way_table(workspace, table_name, standard_fields):
    way_tag_table = os.path.join(workspace, table_name)
    arcpy.CreateTable_management(workspace, table_name)
    # arcpy.CreateFeatureclass_management(workspace, table_name, "POLYLINE", "#", "DISABLED", "DISABLED",
    #                                     COORDINATES_SYSTEM)
    for field in WAY_SAVED_ATTRIBUTES:
        arcpy.AddField_management(way_tag_table, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(way_tag_table, "OID@", standard_fields, "_ID")
    # arcpy.AddField_management(way_tag_table, 'has_attributes', 'STRING', "#", "#", 3)
    # arcpy.AddField_management(way_tag_table, 'is_closed', 'STRING', "#", "#", 3)
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
def create_multipolygon_table(workspace, feature_class_name, standard_fields):
    multipolygon_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "POLYGON", "#", "DISABLED", "DISABLED",
                                        COORDINATES_SYSTEM)
    for field in WAY_SAVED_ATTRIBUTES:
        arcpy.AddField_management(multipolygon_feature_class, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(multipolygon_feature_class, "OID@", standard_fields, "_ID")
    return multipolygon_feature_class


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
def import_osm(osm_file, output_geodatabase, nodes_feature_class, csv_nodes_path, way_feature_class, csv_way_nodes, relations_table, relations_members, polygon_feature_class, multipolygon_feature_class, multipolygon_temporary_file):
    node_base_attr = [field.name for field in NODE_SAVED_ATTRIBUTES]
    node_all_attr = ['SHAPE@XY'] + node_base_attr + STANDARD_FIELDS_ARRAY

    way_base_attr = [field.name for field in WAY_SAVED_ATTRIBUTES]
    way_tags_all_attr = way_base_attr + STANDARD_FIELDS_ARRAY

    relations_base_attr = [field.name for field in WAY_SAVED_ATTRIBUTES]
    relations_all_attr = relations_base_attr + RELATION_FIELD_ARRAY

    count_nodes = 0
    count_ways_with_attributes = 0
    count_ways_without_attributes = 0

    # Edit session is required to edit multiple feature class at a time within the same workspace
    with arcpy.da.Editor(output_geodatabase) as edit:
        with arcpy.da.InsertCursor(nodes_feature_class, node_all_attr) as insert_nodes_cursor:
            with open(csv_nodes_path, 'wb') as csv_nodes_file:
                with arcpy.da.InsertCursor(way_feature_class, way_tags_all_attr) as insert_way_line_cursor:
                    with open(csv_way_nodes, 'wb') as csv_way_nodes_file:
                        with arcpy.da.InsertCursor(relations_table, relations_all_attr) as relations_table_cursor:
                            with arcpy.da.InsertCursor(relations_members, [NODE_ID_FIELD.name,'type','ref','role']) as relations_members_table_cursor:
                                with arcpy.da.InsertCursor(polygon_feature_class, way_tags_all_attr) as polygon_geom_cursor:
                                    with arcpy.da.InsertCursor(multipolygon_feature_class, way_tags_all_attr) as multipolygon_geom_cursor:

                                        csv_nodes_file_writer = csv.writer(csv_nodes_file, delimiter=CSV_DELIMITER)
                                        arcpy.AddMessage('CSV node ways file: {}'.format(csv_way_nodes_file))
                                        way_nodes_writer = csv.writer(csv_way_nodes_file, delimiter=CSV_DELIMITER)

                                        for event, elem in etree.iterparse(osm_file):
                                            if elem.tag == 'node':
                                                point_geom = [float(elem.attrib['lon']), float(elem.attrib['lat'])]
                                                if len(elem) > 0:
                                                    tag_dict = parse_node_children(elem)
                                                    attrib_values = [point_geom]
                                                    for attr in node_base_attr:
                                                        attrib_values.append(elem.attrib[attr])
                                                    for key in STANDARD_FIELDS_ARRAY:
                                                        if key in tag_dict:
                                                            attrib_values.append(tag_dict[key])
                                                        else:
                                                            attrib_values.append(None)

                                                    insert_nodes_cursor.insertRow(attrib_values)
                                                # nodes_dict[elem.attrib['id']] = point_geom
                                                csv_nodes_file_writer.writerow([
                                                    elem.attrib['id'],
                                                    elem.attrib['lon'],
                                                    elem.attrib['lat']]
                                                )
                                                elem.clear()

                                            elif elem.tag == 'way':
                                                tag_dict, nodes = parse_way_children(elem)
                                                if len(nodes) >= 2:
                                                    attrib_values = []

                                                    # Add the attributes associated with the tag
                                                    for attr in way_base_attr:
                                                        attrib_values.append(elem.attrib[attr])

                                                    # Add the attributes coming from children tags
                                                    for key in STANDARD_FIELDS_ARRAY:
                                                        if key in tag_dict:
                                                            attrib_values.append(tag_dict[key])
                                                        else:
                                                            attrib_values.append(None)

                                                    insert_way_line_cursor.insertRow(attrib_values)
                                                    empty_coordinates = ['' for node in nodes]

                                                    is_highway = 'n'
                                                    if 'highway' in tag_dict and tag_dict['highway'] != '':
                                                        is_highway = 'y'
                                                    way_nodes_writer.writerow([
                                                        elem.attrib['id'],
                                                        IDENTIFIER_DELIMITER.join(nodes),
                                                        IDENTIFIER_DELIMITER.join(empty_coordinates),
                                                        is_highway
                                                    ])

                                                    elem.clear()
                                                else:
                                                    print 'Detected way with less than 2 nodes'

                                            # TODO Write relations to csv file. List of
                                            elif elem.tag == 'relation':
                                                tag_dict, members, type = parse_relation_children(elem)
                                                attrib_values = []
                                                for attr in way_base_attr:
                                                    attrib_values.append(elem.attrib[attr])

                                                for key in STANDARD_FIELDS_ARRAY:
                                                    if key in tag_dict:
                                                        attrib_values.append(tag_dict[key])
                                                    else:
                                                        attrib_values.append(None)

                                                members_id = []
                                                for member in members:
                                                    if member.attrib['type'] == 'way':
                                                        members_id.append(member.attrib['ref'])
                                                    member.clear()

                                                if 'type' in tag_dict:
                                                    if tag_dict['type'] == 'multipolygon':
                                                        multipolygon_geom_cursor.insertRow(attrib_values)
                                                        multipolygon_temporary_file.write(
                                                            '{}|{}\n'.format(elem.attrib[NODE_ID_FIELD.name], ','.join(members_id))
                                                        )
                                                elem.clear()

            arcpy.AddMessage('Ways with attributes: {}, Ways with no attributes: {}'.format(
                count_ways_with_attributes,
                count_ways_without_attributes
            ))



###################################
# FUNCTIONS TO BUILD LINES
###################################
@timeit
def build_ways(csv_nodes_path, csv_way_nodes, csv_built_ways, csv_built_areas):
    nodes_chunk = 500000
    nodes_read = 0
    arcpy.AddMessage('Building ways')
    node_dict = {}
    with open(csv_nodes_path, 'rb') as csv_nodes:
        nodes_reader = csv.reader(csv_nodes, delimiter=CSV_DELIMITER)
        for node_row in nodes_reader:
            if nodes_read % nodes_chunk == 0 and nodes_read > 1:
                arcpy.AddMessage('New node chunk: {}'.format(nodes_read))
                # Call function to process chunk
                process_way_chunk(node_dict, csv_way_nodes, csv_built_ways, csv_built_areas)
                node_dict.clear()
            node_dict[node_row[0]] = [float(node_row[1]), float(node_row[2])]
            nodes_read += 1

    # Call function to process chunk
    if len(node_dict) > 0:
        process_way_chunk(node_dict, csv_way_nodes, csv_built_ways, csv_built_areas)


def process_way_chunk(nodes_dict, csv_way_nodes, csv_built_ways, csv_built_areas):
    csv_way_nodes_temp = csv_way_nodes + '_temp'
    with open(csv_way_nodes, 'rb') as csv_way_nodes_file:
        with open(csv_way_nodes_temp, 'wb') as csv_way_nodes_file_temp:
            with open(csv_built_ways, 'wb') as csv_built_ways_file:
                with open(csv_built_areas, 'wb') as csv_built_areas_file:
                    reader = csv.reader(csv_way_nodes_file, delimiter=CSV_DELIMITER)
                    writer = csv.writer(csv_way_nodes_file_temp, delimiter=CSV_DELIMITER)
                    build_way_writer = csv.writer(csv_built_ways_file, delimiter=CSV_DELIMITER)
                    build_areas_writer = csv.writer(csv_built_areas_file, delimiter=CSV_DELIMITER)
                    for row in reader:
                        id = row[0]
                        nodes = row[1].split(IDENTIFIER_DELIMITER)
                        coordinates = row[2].split(IDENTIFIER_DELIMITER)
                        is_linear = row[3]

                        completed = True
                        for index in range(len(nodes)):
                            node_id = nodes[index]
                            if node_id in nodes_dict:
                                node = nodes_dict[node_id]
                                coordinates[index] = '{} {}'.format(node[0], node[1])
                            else:
                                coordinate = coordinates[index]
                                if coordinate is None or coordinate == '':
                                    completed = False

                        if completed:
                            csv_array = [
                                id,
                                IDENTIFIER_DELIMITER.join(coordinates)
                            ]
                            if nodes[-1] == nodes[0] and is_linear == 'n':
                                build_areas_writer.writerow(csv_array)
                            else:
                                build_way_writer.writerow(csv_array)
                        else:
                            csv_array = [
                                id,
                                IDENTIFIER_DELIMITER.join(nodes),
                                IDENTIFIER_DELIMITER.join(coordinates),
                                is_linear
                            ]

                            writer.writerow(csv_array)

    os.remove(csv_way_nodes)
    os.rename(csv_way_nodes_temp, csv_way_nodes)


@timeit
def build_lines(line_feature_class, build_ways_path):
    """
    Insert the content of the a csv file into a line feature class. The csv file is expected to contain an id and
    a sequence of coordinates as text.
    :param line_feature_class: The line feature class.
    :param build_ways_path: The csv that contains the line features definition.
    :return: None.
    """
    count = 0
    with arcpy.da.Editor(output_geodatabase) as edit:
        with arcpy.da.InsertCursor(line_feature_class, [NODE_ID_FIELD.name, 'SHAPE@']) as insert_cursor:
            with open(build_ways_path, 'rb') as build_ways_path_file:
                csv_reader = csv.reader(build_ways_path_file, delimiter=CSV_DELIMITER)
                for row in csv_reader:
                    geometry_txt = row[1].split(IDENTIFIER_DELIMITER)
                    geometries = [g.split(' ') for g in geometry_txt]
                    # points_array = [arcpy.Point(float(geom[0]), float(geom[1])) for geom in geometries]
                    # insert_cursor.insertRow((row[0], arcpy.Polyline(arcpy.Array(points_array))))
                    point_array = []
                    for geom in geometries:
                        point_array.append((float(geom[0]), float(geom[1])))
                    insert_cursor.insertRow((row[0], point_array))
                    count += 1

    arcpy.AddMessage('Inserted {} line geometries'.format(count))

@timeit
def build_polygons(polygon_feature_class, built_areas_path):
    """
    Insert the content of the a csv file into a polygon feature class. The csv file is expected to contain an id and
    a sequence of coordinates as text. The first coordinates in the sequence must be the same than the last coordinates.
    :param polygon_feature_class: The line feature class.
    :param built_areas_path: The csv that contains the polygon features definition.
    :return: None.
    """
    count = 0

    with arcpy.da.Editor(output_geodatabase) as edit:
        with arcpy.da.InsertCursor(polygon_feature_class, [NODE_ID_FIELD.name, 'SHAPE@']) as insert_cursor:
            with open(built_areas_path, 'rb') as build_ways_path_file:
                csv_reader = csv.reader(build_ways_path_file, delimiter=CSV_DELIMITER)
                for row in csv_reader:
                    geometry_txt = row[1].split(IDENTIFIER_DELIMITER)
                    geometries = [g.split(' ') for g in geometry_txt]
                    point_array = []
                    for geom in geometries:
                        point_array.append((float(geom[0]), float(geom[1])))
                    insert_cursor.insertRow((row[0], point_array))
                    count += 1
    arcpy.AddMessage('Inserted {} polygon geometries'.format(count))

@timeit
def load_multipolygon_relations(multipolygons, multipolygon_member_temp_file, ways):
    multipolygon_member_temp_file.seek(0)
    with arcpy.da.UpdateCursor(multipolygons, [NODE_ID_FIELD.name, 'SHAPE@']) as multipolygon_update_cursor:
        for row in multipolygon_update_cursor:
            line = multipolygon_member_temp_file.readline().rstrip('\n')
            infos = line.split('|')
            if row[0] != infos[0]:
                arcpy.AddError('Identifiers mismatch: {} - {}'.format(row[0], infos[0]))

            # Find the way members associated with this relation.Identifiers here should pertain only to ways elements.
            member_identifiers = infos[1].split(',')
            if len(member_identifiers) == 0:
                arcpy.AddWarning('Relation with identifiers does not have any way identifier. Disregarded')
                continue

            # Fetch the ways associated with the identifiers
            where_clause = '{} IN (\'{}\')'.format(
                NODE_ID_FIELD.name,
                '\',\''.join(member_identifiers)
            )
            # arcpy.AddMessage(where_clause)
            shape = arcpy.Array()
            test = []
            shape_added = 0
            with arcpy.da.SearchCursor(ways, [NODE_ID_FIELD.name, 'SHAPE@'], where_clause=where_clause) as ways_search_cursor:
                for way_row in ways_search_cursor:
                    if way_row[1] is not None:
                        # part = way_row[1][0]
                        for part in way_row[1]:
                            # test.append([[p.X, p.Y] for p in part])
                            shape.add(part)
                            shape_added += 1

            if shape_added > 0:
                row[1] = arcpy.Polygon(shape, COORDINATES_SYSTEM)
                multipolygon_update_cursor.updateRow(row)
            # else:
            #     arcpy.AddWarning('No suitable geometries for relation with id: {}'.format(row[0]))


@timeit
def copy_polygon_to_feature_class(ways, polygons):
    fields = [f.name for f in WAY_SAVED_ATTRIBUTES] + STANDARD_FIELDS_ARRAY + ['SHAPE@']
    # arcpy.AddMessage('Fields: {}'.format(fields))
    where_clause = 'is_closed = \'{0}\' AND has_attributes = \'{0}\' AND highway IS NULL'.format(BOOLEAN_YES)
    # is_closed = 'YES' AND has_attributes = 'YES' AND highway <> ''
    # arcpy.AddMessage(where_clause)

    count = 0
    time_start = time.time()
    with arcpy.da.InsertCursor(polygons, fields) as polygon_insert_cursor:
        with arcpy.da.SearchCursor(ways, fields, where_clause) as ways_search_cursor:
            for way_row in ways_search_cursor:
                if count % 10000 == 0:
                    time_end = time.time()
                    arcpy.AddMessage('Exported {} polygons in {}'.format(count, time_end - time_start))
                    time_start = time_end
                new_row = [value for value in way_row]
                new_row[-1] = arcpy.Polygon(way_row[-1].getPart(0), COORDINATES_SYSTEM)
                count += 1

                polygon_insert_cursor.insertRow(new_row)

@timeit
def join_way_attribute(geometry_feature_class, attribute_table, output_feature_class):

    # feature_class_name = arcpy.Describe(geometry_feature_class).name
    # Make a feature class layer
    feature_class_layer = 'v_feature_class'
    arcpy.MakeFeatureLayer_management(geometry_feature_class, feature_class_layer)

    try:
        arcpy.AddJoin_management(
            feature_class_layer,
            NODE_ID_FIELD.name,
            attribute_table,
            NODE_ID_FIELD.name,
            join_type='KEEP_COMMON'
        )

        table_base_name = arcpy.Describe(attribute_table).name

        fieldMappings = arcpy.FieldMappings()
        fields = arcpy.ListFields(feature_class_layer)
        for f in fields:
            alias_name = f.aliasName
            alias_info = alias_name.split('.')

            field_name = f.baseName
            if f.baseName.upper() not in ['OBJECTID', 'SHAPE']:
                if not (alias_info[0].lower() == table_base_name.lower() and field_name == NODE_ID_FIELD.name):
                    field_map = arcpy.FieldMap()
                    field_map.addInputField(feature_class_layer, f.aliasName)
                    output_field = field_map.outputField
                    output_field.name = field_name
                    output_field.aliasName = field_name
                    field_map.outputField = output_field
                    fieldMappings.addFieldMap(field_map)

        arcpy.FeatureClassToFeatureClass_conversion(
            feature_class_layer,
            os.path.dirname(output_feature_class),
            os.path.basename(output_feature_class),
            field_mapping=fieldMappings
        )
        arcpy.RemoveJoin_management(feature_class_layer)

    except Exception as e:
        raise
    finally:
        arcpy.Delete_management(feature_class_layer)

# def copy_polygon_to_feature_class(ways, polygons):
#     fields = [f.name for f in WAY_SAVED_ATTRIBUTES] + STANDARD_FIELDS_ARRAY + ['SHAPE@']
#     # arcpy.AddMessage('Fields: {}'.format(fields))
#     where_clause = 'is_closed = \'{0}\' AND has_attributes = \'{0}\' AND highway IS NULL'.format(BOOLEAN_YES)
#     layer_polygons = 'v_ways_polygons'
#     field_info = arcpy.FieldInfo()
#     for field in arcpy.ListFields():
#         visible = 'VISIBLE'
#         if field.name in ['is_closed', 'has_attributes']:
#             visible = 'HIDDEN'
#         field_info.addField(field.name, field.name, visible, 'NONE')
#
#     arcpy.MakeFeatureLayer_management()
#
#     # is_closed = 'YES' AND has_attributes = 'YES' AND highway <> ''
#     # arcpy.AddMessage(where_clause)
#     with arcpy.da.InsertCursor(polygons, fields) as polygon_insert_cursor:
#         with arcpy.da.SearchCursor(ways, fields, where_clause) as ways_search_cursor:
#             for way_row in ways_search_cursor:
#                 new_row = [value for value in way_row]
#                 new_row[-1] = arcpy.Polygon(way_row[-1].getPart(0), COORDINATES_SYSTEM)
#
#                 polygon_insert_cursor.insertRow(tuple(new_row))


@timeit
def copy_lines_to_feature_class(temp_ways, ways):
    v_layers = 'v_ways'
    where_clause = 'NOT (is_closed = \'{0}\' AND has_attributes = \'{0}\' AND highway IS NULL)'.format(BOOLEAN_YES)
    field_infos = arcpy.FieldInfo()
    for field in arcpy.ListFields(temp_ways):
        visible = 'VISIBLE'
        if field.name.lower() in ['is_closed', 'has_attributes']:
            visible = 'HIDDEN'
        field_infos.addField(field.name, field.name, visible, 'NONE')

    arcpy.MakeFeatureLayer_management(temp_ways, v_layers, where_clause, field_info=field_infos)
    arcpy.CopyFeatures_management(v_layers, ways)
    # arcpy.Delete_management(temp_ways)

###################################
# MAIN CALLING FUNCTION
###################################
def process(osm_file, output_geodatabase, temporary_file):
    create_output_workspace(output_geodatabase)

    additional_fields = get_fields_numpy_definition(STANDARD_FIELDS_ARRAY)
    additional_fields_relations = get_fields_numpy_definition(RELATION_FIELD_ARRAY)
    nodes_feature_class = create_node_feature_class(output_geodatabase, 'nodes', additional_fields)
    polygon_feature_class = create_multipolygon_table(output_geodatabase, 'polygons', additional_fields)
    multipolygon_feature_class = create_multipolygon_table(output_geodatabase, 'multipolygons', additional_fields)
    way_line_geom_feature_class = create_way_line_geom_feature_class(output_geodatabase, 'ways_line_geom')
    way_polygon_geom_feature_class = create_way_polygon_geom_feature_class(output_geodatabase, 'ways_polygon_geom')

    way_attr_table = create_way_table(output_geodatabase, 'temp_ways', additional_fields)
    way_nodes_table_lines = create_way_nodes(output_geodatabase, 'way_nodes')
    relations_table = create_relations_table(output_geodatabase, 'relations', additional_fields_relations)
    relations_members = create_relations_member(output_geodatabase, 'relations_members')

    csv_nodes = os.path.join(temporary_file, CSV_NODES)
    csv_way_nodes = os.path.join(temporary_file, CSV_WAY_NODES)
    csv_built_ways = os.path.join(temporary_file, 'built_ways.csv')
    csv_built_areas = os.path.join(temporary_file, 'built_areas.csv')

    with tempfile.TemporaryFile() as multipolygon_temporary_file:
        # Parse the XML file
        import_osm(
                bz2.BZ2File(osm_file, 'r'),
                output_geodatabase,
                nodes_feature_class,
                csv_nodes,
                way_attr_table,
                csv_way_nodes,
                relations_table,
                relations_members,
                polygon_feature_class,
                multipolygon_feature_class,
                multipolygon_temporary_file
        )

        arcpy.AddIndex_management(
            nodes_feature_class,
            [NODE_ID_FIELD.name],
            index_name='{}_idx'.format(NODE_ID_FIELD.name),
            unique=True)

        arcpy.AddIndex_management(
            way_attr_table,
            [NODE_ID_FIELD.name],
            index_name='{}_idx'.format(NODE_ID_FIELD.name),
            unique=True)

        arcpy.AddIndex_management(
            way_polygon_geom_feature_class,
            [NODE_ID_FIELD.name],
            index_name='{}_idx'.format(NODE_ID_FIELD.name),
            unique=True)

        arcpy.AddIndex_management(
            way_attr_table,
            ['is_closed', 'has_attributes', 'highway'],
            index_name='temporary_fields_idx',
            unique=False)

        # Parse the csv files and associated nodes identifier with way nodes.
        build_ways(
            csv_nodes,
            csv_way_nodes,
            csv_built_ways,
            csv_built_areas
        )

        # Build the lines geometries - no attributes
        build_lines(
            way_line_geom_feature_class,
            csv_built_ways
        )

        join_way_attribute(
            way_line_geom_feature_class,
            way_attr_table,
            os.path.join(output_geodatabase, 'way_lines_final')
        )

        # Build the polygons geometries - no attributes
        build_polygons(
            way_polygon_geom_feature_class,
            csv_built_areas
        )

        join_way_attribute(
            way_polygon_geom_feature_class,
            way_attr_table,
            os.path.join(output_geodatabase, 'way_polygons_final')
        )

        # Load the multipolygon
        load_multipolygon_relations(
            multipolygon_feature_class,
            multipolygon_temporary_file,
            way_polygon_geom_feature_class
        )

        #
        # copy_polygon_to_feature_class(way_feature_class, multipolygon_feature_class)
        #
        # output_ways = os.path.join(output_geodatabase, 'ways')
        # arcpy.AddMessage(output_ways)
        # copy_lines_to_feature_class(way_feature_class, output_ways)




if __name__ == '__main__':
    # osm_file = r'D:\Temp\Custom OSM Parser\monaco-latest.osm.bz2'
    # output_geodatabase = r'D:\Temp\Custom OSM Parser\monaco-latest.gdb'
    osm_file = arcpy.GetParameterAsText(0)
    output_geodatabase = arcpy.GetParameterAsText(1)
    process(osm_file, output_geodatabase, r'D:\Temp\Custom OSM Parser')

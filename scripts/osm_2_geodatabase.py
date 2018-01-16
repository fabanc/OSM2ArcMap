# "D:\Temp\Custom OSM Parser\luxembourg-latest.osm.bz2" "D:\Temp\Custom OSM Parser\luxembourg-latest.gdb" "D:\Temp\Custom OSM Parser" 500000
# "D:\Temp\Custom OSM Parser\monaco-latest.osm.bz2" "D:\Temp\Custom OSM Parser\monaco-latest.gdb"
import os, time, bz2, tempfile, time, csv, itertools, datetime, arcpy, numpy


try:
    from lxml import etree
except:
    import xml.etree.ElementTree as etree


arcpy.env.overwriteOutput = True

STANDARD_FIELDS = set((
    'highway', 'name', 'name_en', 'ref', 'lanes', 'surface', 'oneway', 'maxspeed', 'tracktype', 'access', 'service',
    'foot', 'bicycle', 'bridge', 'barrier', 'lit', 'layer', 'building', 'building_levels', 'building_height',
    'addr_housenumber', 'addr_street', 'addr_city', 'addr_postcode', 'addr_country', 'addr_place', 'addr_state',
    'natural', 'landuse', 'waterway', 'power', 'amenity', 'place', 'height', 'note', 'railway', 'public_transport',
    'operator', 'guage', 'width', 'tunnel', 'leisure', 'is_in', 'ele', 'shop', 'man_made', 'parking', 'boundary',
    'aerialway', 'aeroway', 'craft', 'emergency', 'geological', 'historic', 'military', 'office', 'sport', 'tourism',
    'traffic_calming', 'entrance', 'crossing'
))

STANDARD_FIELDS_ARRAY = list(STANDARD_FIELDS)

ID_FIELD = arcpy.Field()
ID_FIELD.name = 'id'
ID_FIELD.type = 'String'
ID_FIELD.length = '30'

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
    ID_FIELD,
    NODE_LON_FIELD,
    NODE_LAT_FIELD,
    TIMESTAMP_FIELD
]

WAY_SAVED_ATTRIBUTES = [
    ID_FIELD,
    TIMESTAMP_FIELD
]

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
        ts = datetime.datetime.now().replace(microsecond=0)
        result = method(*args, **kw)
        te = datetime.datetime.now().replace(microsecond=0)
        arcpy.AddMessage('Method {} executed in {} (hours:minutes:seconds)'.format(method.__name__, te - ts))
        return result

    return timed


def get_fields_numpy_definition(field_list):
    """
    Make a numpy array that can be used to add attribute to a table or feature class.
    All fields are created as text fields with a length of 255.
    :param field_list: An iterable collection of field names. Field names must occur once.
    :return: the numpy array.
    """
    standard_fields_array_tuple = [('_ID', numpy.int)]
    for f in field_list:
        standard_fields_array_tuple.append((f, '|S255'))

    return numpy.array(
        [],
        numpy.dtype(standard_fields_array_tuple)
    )


def create_output_workspace(workspace):
    """
    Create an output file geodatabase if it does not already exist.
    :param workspace: The workspace to create.
    :return:
    """
    split_path = os.path.split(workspace)
    if not arcpy.Exists(workspace):
        arcpy.CreateFileGDB_management(split_path[0], split_path[1])


###################################
# PARSING FUNCTIONS FOR NODES, WAYS, AND RELATIONSHIPS
###################################
def parse_node_children(elem):
    return {child.attrib['k']: child.attrib['v'] for child in elem if child.tag == 'tag'}


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
    way_members = []
    elem_type = None
    for child in elem:
        if child.tag == 'tag':
            tag_dict[child.attrib['k']] = child.attrib['v']
            if child.attrib['k'] == 'type':
                elem_type = child.attrib['v']

        elif child.tag == 'member':
            # members.append(child)
            if child.attrib['type'] == 'way':
                way_members.append(child.attrib['ref'])

        child.clear()
    return tag_dict, members, elem_type, way_members

###################################
# FUNCTIONS TO CREATE FEATURE CLASS
###################################


@timeit
def create_node_feature_class(workspace, feature_class_name, standard_fields):
    """
    Create the node feature class table. The table is created with a set of valid tags.
    :param workspace: The geodatabase where the feature class will be created.
    :param feature_class_name: The name of the output feature class.
    :param standard_fields: The numpy array representing the OSM attribute fields
    :return: The full path to the feature class.
    """
    node_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(
        workspace,
        feature_class_name,
        "point",
        "#",
        "DISABLED",
        "DISABLED",
        COORDINATES_SYSTEM
    )

    # The list of xml element attributes that will be added as
    for field in NODE_SAVED_ATTRIBUTES:
        arcpy.AddField_management(node_feature_class, field.name, field.type, "#", "#", field.length)

    # The fields for the "tag" children xml elements of each "node" element
    arcpy.da.ExtendTable(node_feature_class, "OID@", standard_fields, "_ID")
    return node_feature_class


@timeit
def create_way_line_geom_feature_class(workspace, feature_class_name):
    """
    Create the feature class that will contain only the line geometries and their OSM identifiers.
    :param workspace: The geodatabase where the feature class will be created.
    :param feature_class_name: The name of the output feature class.
    :return: The full path to the feature class.
    """
    way_tag_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "POLYLINE", "#", "DISABLED", "DISABLED",COORDINATES_SYSTEM)
    arcpy.AddField_management(way_tag_feature_class, ID_FIELD.name, 'STRING', "#", "#", ID_FIELD.length)
    return way_tag_feature_class


@timeit
def create_way_polygon_geom_feature_class(workspace, feature_class_name):
    """
    Create the feature class that will contain only the polygon geometries and their OSM identifiers.
    :param workspace: The geodatabase where the feature class will be created.
    :param feature_class_name: The name of the output feature class.
    :return: The full path to the feature class.
    """
    way_tag_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "POLYGON", "#", "DISABLED", "DISABLED",COORDINATES_SYSTEM)
    arcpy.AddField_management(way_tag_feature_class, ID_FIELD.name, 'STRING', "#", "#", ID_FIELD.length)
    return way_tag_feature_class


@timeit
def create_way_table(workspace, table_name, standard_fields):
    """
    Create a table that will contains the tag associated with way elements.
    :param workspace: The geodatabase where the feature class will be created.
    :param table_name: The name of the output table.
    :param standard_fields: The numpy array representing the OSM attribute fields
    :return: The full path to the table.
    """
    way_tag_table = os.path.join(workspace, table_name)
    arcpy.CreateTable_management(workspace, table_name)
    for field in WAY_SAVED_ATTRIBUTES:
        arcpy.AddField_management(way_tag_table, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(way_tag_table, "OID@", standard_fields, "_ID")
    return way_tag_table


@timeit
def create_multipolygon_table(workspace, feature_class_name, standard_fields):
    """

    :param workspace: The geodatabase where the feature class will be created.
    :param feature_class_name: The name of the output feature class.
    :param standard_fields: The numpy array representing the OSM attribute fields
    :return: The full path to the feature class.
    """
    multipolygon_feature_class = os.path.join(workspace, feature_class_name)
    arcpy.CreateFeatureclass_management(workspace, feature_class_name, "POLYGON", "#", "DISABLED", "DISABLED",
                                        COORDINATES_SYSTEM)
    for field in WAY_SAVED_ATTRIBUTES:
        arcpy.AddField_management(multipolygon_feature_class, field.name, field.type, "#", "#", field.length)
    arcpy.da.ExtendTable(multipolygon_feature_class, "OID@", standard_fields, "_ID")
    return multipolygon_feature_class


@timeit
def create_relations_member(workspace, table_name):
    relations_table = os.path.join(workspace, table_name)
    arcpy.CreateTable_management(workspace, table_name)
    arcpy.AddField_management(relations_table, ID_FIELD.name, ID_FIELD.type, "#", "#", ID_FIELD.length)
    arcpy.AddField_management(relations_table, 'type', 'STRING', "#", "#", 30)
    arcpy.AddField_management(relations_table, 'ref', ID_FIELD.type, "#", "#", ID_FIELD.length)
    arcpy.AddField_management(relations_table, 'role', 'STRING', "#", "#", 30)
    return relations_table


###################################
# PARSING FUNCTION
###################################
@timeit
def import_osm(osm_file, output_geodatabase, nodes_feature_class, csv_nodes_path, way_feature_class, csv_way_nodes, multipolygon_feature_class, multipolygon_temporary_file):
    """
    Parse the OSM file and put the relevant information into temporaries csv files and feature class.
    :param osm_file: The path to the xml file compressed as bz2.
    :param output_geodatabase: The geodatabase that contains the temporary feature class
    (used for controlling the edit session)
    :param nodes_feature_class: The feature class the contains nodes and their attributes. Only nodes with
    attributes will be written here.
    :param csv_nodes_path: The path to the csv file that will contain the association between ways and nodes.
    :param way_feature_class: The feature class that will contain the line geometries.
    :param csv_way_nodes: The csv file that will contain the association between ways and nodes.
    :param multipolygon_feature_class: The feature class that will contain the multipolygons and their tags.
    :param multipolygon_temporary_file: The temporary files used to write multipolygons components.
    :return:
    """
    # Local copies of global variable. Referencing local variables in faster in python than global ones.
    standard_fields_array = STANDARD_FIELDS_ARRAY
    identifier_delimiter = IDENTIFIER_DELIMITER

    node_base_attr = [field.name for field in NODE_SAVED_ATTRIBUTES]
    node_all_attr = ['SHAPE@XY'] + node_base_attr + standard_fields_array

    way_base_attr = [field.name for field in WAY_SAVED_ATTRIBUTES]
    way_tags_all_attr = way_base_attr + standard_fields_array

    count_nodes = 0
    count_nodes_with_attributes = 0
    count_ways = 0
    count_ways_with_attributes = 0
    count_multipolygons = 0

    # Edit session is required to edit multiple feature class at a time within the same workspace
    with arcpy.da.Editor(output_geodatabase) as edit:
        with arcpy.da.InsertCursor(nodes_feature_class, node_all_attr) as insert_nodes_cursor:
            with open(csv_nodes_path, 'w') as csv_nodes_file:
                with arcpy.da.InsertCursor(way_feature_class, way_tags_all_attr) as insert_way_line_cursor:
                    with open(csv_way_nodes, 'w') as csv_way_nodes_file:
                        with arcpy.da.InsertCursor(multipolygon_feature_class, way_tags_all_attr) as multipolygon_cursor:
                            csv_nodes_file_writer = csv.writer(csv_nodes_file, delimiter=CSV_DELIMITER)
                            way_nodes_writer = csv.writer(csv_way_nodes_file, delimiter=CSV_DELIMITER)

                            parent = None
                            for event, elem in etree.iterparse(osm_file, events=('start', 'end')):
                                if event == 'start':
                                    if parent is None and elem.tag == 'osm':
                                        parent = elem
                                else:
                                    if elem.tag == 'node':
                                        point_geom = [float(elem.attrib['lon']), float(elem.attrib['lat'])]
                                        if len(elem) > 0:
                                            tag_dict = parse_node_children(elem)
                                            attrib_values = [point_geom]
                                            for attr in node_base_attr:
                                                attrib_values.append(elem.attrib[attr])
                                            for key in standard_fields_array:
                                                if key in tag_dict:
                                                    attrib_values.append(tag_dict[key])
                                                else:
                                                    attrib_values.append(None)

                                            insert_nodes_cursor.insertRow(attrib_values)
                                            count_nodes_with_attributes += 1

                                        csv_nodes_file_writer.writerow([
                                            elem.attrib['id'],
                                            elem.attrib['lon'],
                                            elem.attrib['lat']]
                                        )

                                        count_nodes += 1
                                        if count_nodes % 1000000 == 0:
                                            arcpy.AddMessage(
                                                'Loaded {0} nodes ... Still loading ...'.format(count_nodes)
                                            )
                                        elem.clear()
                                        parent.remove(elem)

                                    elif elem.tag == 'way':
                                        tag_dict, nodes = parse_way_children(elem)

                                        if len(nodes) >= 2:
                                            attrib_values = [elem.attrib[attr] for attr in way_base_attr]

                                            # Add the attributes coming from children tags
                                            for key in standard_fields_array:
                                                if key in tag_dict:
                                                    attrib_values.append(tag_dict[key])
                                                else:
                                                    attrib_values.append(None)

                                            if len(tag_dict) > 0:
                                                count_ways_with_attributes += 1
                                                insert_way_line_cursor.insertRow(attrib_values)

                                            empty_coordinates = ['' for node in nodes]

                                            is_highway = 'n'
                                            if 'highway' in tag_dict and tag_dict['highway'] != '':
                                                is_highway = 'y'

                                            count_ways += 1
                                            if count_ways % 1000000 == 0:
                                                arcpy.AddMessage(
                                                    'Loaded {} ways ... Still loading ...'.format(count_ways)
                                                )

                                            way_nodes_writer.writerow([
                                                elem.attrib['id'],
                                                identifier_delimiter.join(nodes),
                                                identifier_delimiter.join(empty_coordinates),
                                                is_highway
                                            ])

                                        else:
                                            arcpy.AddWarning('Way with id {} has less than 2 nodes'.format(
                                                elem.attrib['id'])
                                            )

                                        elem.clear()
                                        parent.remove(elem)
                                        # toto = 'Toto'

                                    elif elem.tag == 'relation':
                                        tag_dict, members, elem_type, way_members = parse_relation_children(elem)

                                        attrib_values = [elem.attrib[attr] for attr in way_base_attr]

                                        for key in standard_fields_array:
                                            if key in tag_dict:
                                                attrib_values.append(tag_dict[key])
                                            else:
                                                attrib_values.append(None)

                                        if 'type' in tag_dict:
                                            if tag_dict['type'] == 'multipolygon':
                                                count_multipolygons += 1
                                                multipolygon_cursor.insertRow(attrib_values)
                                                multipolygon_temporary_file.write(
                                                    '{}|{}\n'.format(elem.attrib[ID_FIELD.name], ','.join(way_members))
                                                )
                                        elem.clear()
                                        parent.remove(elem)

            arcpy.AddMessage('Imported {} nodes. {} nodes have attributes.'.format(
                count_nodes,
                count_nodes_with_attributes
            ))

            arcpy.AddMessage('Imported {} ways. {} have attributes.'.format(
                count_ways,
                count_ways_with_attributes
            ))

            arcpy.AddMessage('Derived {} multipolygons from the relations.'.format(
                count_multipolygons
            ))


###################################
# FUNCTIONS TO BUILD LINES
###################################
@timeit
def build_ways(csv_nodes_path, csv_way_nodes, csv_built_ways, csv_built_areas, nodes_chunk_size=500000):
    """
    Derive geometries from way lines and way polygons from the csv representing nodes and way_nodes files
    :param csv_nodes_path: The csv files containing nodes.
    :param csv_way_nodes: The csv files containing the way / nodes association.
    :param csv_built_ways: The output csv files where line geometries will be written
    :param csv_built_areas: The output csv file where polygons geometries will be written.
    :param nodes_chunk_size: The number of nodes loaded in memory at once in memory.
    :return:
    """
    nodes_read = 0
    arcpy.AddMessage('Building ways')
    node_dict = {}

    count_remaining_ways = 0
    count_built_ways = 0
    count_built_areas = 0

    with open(csv_built_ways, 'w') as csv_built_ways_file:
        build_way_csv_writer = csv.writer(csv_built_ways_file, delimiter=CSV_DELIMITER)
        with open(csv_built_areas, 'w') as csv_built_areas_file:
            build_areas_csv_writer = csv.writer(csv_built_areas_file, delimiter=CSV_DELIMITER)
            with open(csv_nodes_path, 'r') as csv_nodes:
                nodes_reader = csv.reader(csv_nodes, delimiter=CSV_DELIMITER)
                for node_row in nodes_reader:
                    node_dict[node_row[0]] = node_row[1] + ' ' + node_row[2]
                    nodes_read += 1
                    if nodes_read % nodes_chunk_size == 0:
                        arcpy.AddMessage('{} nodes read'.format(nodes_read))
                        # Call function to process chunk
                        remaining_ways, built_ways, built_areas = process_way_chunk(
                            node_dict,
                            csv_way_nodes,
                            build_way_csv_writer,
                            build_areas_csv_writer
                        )
                        count_remaining_ways = remaining_ways
                        count_built_ways += built_ways
                        count_built_areas += built_areas
                        node_dict.clear()

            # Call function to process chunk
            if len(node_dict) > 0:
                arcpy.AddMessage('{} nodes read'.format(nodes_read))
                remaining_ways, built_ways, built_areas = process_way_chunk(
                    node_dict,
                    csv_way_nodes,
                    build_way_csv_writer,
                    build_areas_csv_writer
                )
                count_remaining_ways = remaining_ways
                count_built_ways += built_ways
                count_built_areas += built_areas

    if count_remaining_ways > 0:
        arcpy.AddWarning(
            '{} ways have been left unprocessed. This indicates that the nodes for those ways could not be found.'.format(
                count_remaining_ways
            ))

    arcpy.AddMessage('Total built lines: {}, Total built areas: {}'.format(count_built_ways, count_built_areas))


def process_way_chunk(nodes_dict, csv_way_nodes, build_way_csv_writer, build_areas_csv_writer):
    """
    Process a chunk of nodes loaded in-memory. Associated then with ways and write lines and polygons to a csv file.
    :param nodes_dict: A chunk of nodes loaded in memory.
    :param csv_way_nodes: The csv that contains the association between ways and nodes.
    :param build_way_csv_writer: The csv writer files where line geometries will be written
    :param build_areas_csv_writer: The csv writer file where polygons geometries will be written.
    :return:
    """
    # Local copies of global variable. Referencing local variables in faster in python than global ones.
    identifier_delimiter = IDENTIFIER_DELIMITER

    count_remaining_ways = 0
    count_built_ways = 0
    count_built_areas = 0
    csv_way_nodes_temp = csv_way_nodes + '_temp'
    with open(csv_way_nodes, 'r') as csv_way_nodes_file:
        with open(csv_way_nodes_temp, 'w') as csv_way_nodes_file_temp:
                    reader = csv.reader(csv_way_nodes_file, delimiter=CSV_DELIMITER)
                    writer = csv.writer(csv_way_nodes_file_temp, delimiter=CSV_DELIMITER)
                    for row in reader:
                        identifier = row[0]
                        nodes = row[1].split(identifier_delimiter)
                        coordinates = row[2].split(identifier_delimiter)
                        is_linear = row[3]

                        completed = True
                        for index in range(len(nodes)):
                            node_id = nodes[index]
                            if node_id in nodes_dict:
                                node = nodes_dict[node_id]
                                coordinates[index] = node
                            else:
                                coordinate = coordinates[index]
                                if coordinate is None or coordinate == '':
                                    completed = False

                        if completed:
                            csv_array = [
                                identifier,
                                identifier_delimiter.join(coordinates)
                            ]
                            if nodes[-1] == nodes[0] and is_linear == 'n':
                                count_built_areas += 1
                                build_areas_csv_writer.writerow(csv_array)
                            else:
                                count_built_ways += 1
                                build_way_csv_writer.writerow(csv_array)
                        else:
                            count_remaining_ways += 1
                            csv_array = [
                                identifier,
                                row[1],
                                identifier_delimiter.join(coordinates),
                                is_linear
                            ]

                            writer.writerow(csv_array)

    os.remove(csv_way_nodes)
    os.rename(csv_way_nodes_temp, csv_way_nodes)

    arcpy.AddMessage('Statistics for node chunk: {} remaining ways to build, {} built lines, {} built areas'.format(
        count_remaining_ways,
        count_built_ways,
        count_built_areas
    ))

    return count_remaining_ways, count_built_ways, count_built_areas


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
        with arcpy.da.InsertCursor(line_feature_class, [ID_FIELD.name, 'SHAPE@']) as insert_cursor:
            with open(build_ways_path, 'rb') as build_ways_path_file:
                csv_reader = csv.reader(build_ways_path_file, delimiter=CSV_DELIMITER)
                for row in csv_reader:
                    geometry_txt = row[1].split(IDENTIFIER_DELIMITER)
                    geometries = [g.split(' ') for g in geometry_txt]
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
    identifier_delimiter = IDENTIFIER_DELIMITER
    count = 0
    with arcpy.da.Editor(output_geodatabase) as edit:
        with arcpy.da.InsertCursor(polygon_feature_class, [ID_FIELD.name, 'SHAPE@']) as insert_cursor:
            with open(built_areas_path, 'rb') as build_ways_path_file:
                csv_reader = csv.reader(build_ways_path_file, delimiter=CSV_DELIMITER)
                for row in csv_reader:
                    geometry_txt = row[1].split(identifier_delimiter)
                    geometries = [g.split(' ') for g in geometry_txt]
                    point_array = [(float(geom[0]), float(geom[1])) for geom in geometries]
                    insert_cursor.insertRow((row[0], point_array))
                    count += 1
    arcpy.AddMessage('Inserted {} polygon geometries'.format(count))


@timeit
def load_multipolygon_relations(multipolygons, multipolygon_member_temp_file, ways):
    multipolygon_member_temp_file.seek(0)
    with arcpy.da.UpdateCursor(multipolygons, [ID_FIELD.name, 'SHAPE@']) as multipolygon_update_cursor:
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
                ID_FIELD.name,
                '\',\''.join(member_identifiers)
            )

            shape = arcpy.Array()
            shape_added = 0
            with arcpy.da.SearchCursor(ways, [ID_FIELD.name, 'SHAPE@'], where_clause=where_clause) as ways_search_cursor:
                for way_row in ways_search_cursor:
                    if way_row[1] is not None:
                        for part in way_row[1]:
                            shape.add(part)
                            shape_added += 1

            if shape_added > 0:
                row[1] = arcpy.Polygon(shape, COORDINATES_SYSTEM)
                multipolygon_update_cursor.updateRow(row)
            # else:
            #     arcpy.AddWarning('No suitable geometries for relation with id: {}'.format(row[0]))


@timeit
def join_way_attribute(geometry_feature_class, attribute_table, output_feature_class):

    # Make a feature class layer
    feature_class_layer = 'v_feature_class'
    arcpy.MakeFeatureLayer_management(geometry_feature_class, feature_class_layer)

    try:
        arcpy.AddJoin_management(
            feature_class_layer,
            ID_FIELD.name,
            attribute_table,
            ID_FIELD.name,
            join_type='KEEP_COMMON'
        )

        table_base_name = arcpy.Describe(attribute_table).name

        field_mappings = arcpy.FieldMappings()
        fields = arcpy.ListFields(feature_class_layer)
        for f in fields:
            alias_name = f.aliasName
            alias_info = alias_name.split('.')

            field_name = f.baseName
            if f.baseName.upper() not in ['OBJECTID', 'SHAPE']:
                if not (alias_info[0].lower() == table_base_name.lower() and field_name == ID_FIELD.name):
                    field_map = arcpy.FieldMap()
                    field_map.addInputField(feature_class_layer, f.aliasName)
                    output_field = field_map.outputField
                    output_field.name = field_name
                    output_field.aliasName = field_name
                    field_map.outputField = output_field
                    field_mappings.addFieldMap(field_map)

        arcpy.FeatureClassToFeatureClass_conversion(
            feature_class_layer,
            os.path.dirname(output_feature_class),
            os.path.basename(output_feature_class),
            field_mapping=field_mappings
        )
        arcpy.RemoveJoin_management(feature_class_layer)

    except Exception:
        raise
    finally:
        arcpy.Delete_management(feature_class_layer)


@timeit
def append_polygons(source, destination):
    arcpy.Append_management(source, destination)


def process(osm_file, output_geodatabase, processing_folder, nodes_chunk_size=500000):
    """
    The main function. Parse the xml and create the required features from it.
    :param osm_file: The osm file, compressed as bz2
    :param output_geodatabase: The output geodatabase.
    :param processing_folder: The processing folder. This is where temporary files will be created.
    :param nodes_chunk_size: The number of nodes loaded in memory at once when loading nodes.
    :return:
    """

    create_output_workspace(output_geodatabase)

    additional_fields = get_fields_numpy_definition(STANDARD_FIELDS_ARRAY)

    # Temporary feature classes
    multipolygon_feature_class = create_multipolygon_table(output_geodatabase, 'multipolygons', additional_fields)
    way_line_geom_feature_class = create_way_line_geom_feature_class(output_geodatabase, 'ways_line_geom')
    way_polygon_geom_feature_class = create_way_polygon_geom_feature_class(output_geodatabase, 'ways_polygon_geom')
    way_attr_table = create_way_table(output_geodatabase, 'temp_ways', additional_fields)

    # Output feature classes
    output_nodes_feature_class = create_node_feature_class(output_geodatabase, 'nodes', additional_fields)
    output_line_feature_class = os.path.join(output_geodatabase, 'way_lines')
    output_polygon_feature_class = os.path.join(output_geodatabase, 'way_polygons')

    csv_nodes = os.path.join(processing_folder, CSV_NODES)
    csv_way_nodes = os.path.join(processing_folder, CSV_WAY_NODES)
    csv_built_ways = os.path.join(processing_folder, 'built_ways.csv')
    csv_built_areas = os.path.join(processing_folder, 'built_areas.csv')

    csv_to_remove = [csv_nodes, csv_way_nodes, csv_built_ways, csv_built_areas]

    feature_class_to_remove = [
        way_line_geom_feature_class,
        way_polygon_geom_feature_class,
        multipolygon_feature_class,
        way_attr_table
    ]

    with tempfile.TemporaryFile() as multipolygon_temporary_file:
        # Parse the XML file
        import_osm(
                bz2.BZ2File(osm_file, 'r'),
                output_geodatabase,
                output_nodes_feature_class,
                csv_nodes,
                way_attr_table,
                csv_way_nodes,
                multipolygon_feature_class,
                multipolygon_temporary_file
        )

        arcpy.AddIndex_management(
            output_nodes_feature_class,
            [ID_FIELD.name],
            index_name='{}_idx'.format(ID_FIELD.name),
            unique=True)

        arcpy.AddIndex_management(
            way_attr_table,
            [ID_FIELD.name],
            index_name='{}_idx'.format(ID_FIELD.name),
            unique=True)

        arcpy.AddIndex_management(
            way_line_geom_feature_class,
            [ID_FIELD.name],
            index_name='{}_idx'.format(ID_FIELD.name),
            unique=True)

        arcpy.AddIndex_management(
            way_polygon_geom_feature_class,
            [ID_FIELD.name],
            index_name='{}_idx'.format(ID_FIELD.name),
            unique=True)

        # Parse the csv files and associated nodes identifier with way nodes.
        build_ways(
            csv_nodes,
            csv_way_nodes,
            csv_built_ways,
            csv_built_areas,
            nodes_chunk_size
        )

        # Build the lines geometries - no attributes
        build_lines(
            way_line_geom_feature_class,
            csv_built_ways
        )

        join_way_attribute(
            way_line_geom_feature_class,
            way_attr_table,
            output_line_feature_class
        )

        # Build the polygons geometries - no attributes
        build_polygons(
            way_polygon_geom_feature_class,
            csv_built_areas
        )

        join_way_attribute(
            way_polygon_geom_feature_class,
            way_attr_table,
            output_polygon_feature_class
        )

        # Load the multipolygon
        load_multipolygon_relations(
            multipolygon_feature_class,
            multipolygon_temporary_file,
            way_polygon_geom_feature_class
        )

        append_polygons(multipolygon_feature_class, output_polygon_feature_class)

        for csv_path in csv_to_remove:
            if os.path.isfile(csv_path):
                os.remove(csv_path)

        for fc in feature_class_to_remove:
            if arcpy.Exists(fc):
                arcpy.Delete_management(fc)


if __name__ == '__main__':
    # osm_file = r'D:\Temp\Custom OSM Parser\monaco-latest.osm.bz2'
    # output_geodatabase = r'D:\Temp\Custom OSM Parser\monaco-latest.gdb'
    input_osm_file = arcpy.GetParameterAsText(0)
    output_geodatabase = arcpy.GetParameterAsText(1)
    temporary_workspace = arcpy.GetParameterAsText(2)
    nodes_chunk_size = arcpy.GetParameter(3)
    process(input_osm_file, output_geodatabase, temporary_workspace, nodes_chunk_size=int(nodes_chunk_size))

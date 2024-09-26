import json
import os
import glob
import xml.etree.ElementTree as ET
import re
import copy
from Input_Node_replacement import replace_subflow_nodes

increment_tracker = {}
name_increment_tracker = {}  # Tracks names and IDs for all elements.
group_name_tracker = {}  # Tracker for group names

# Method to find and read all .msgflow files in the given directory
def find_and_read_msgflow_files(directory):
    msgflow_files = glob.glob(os.path.join(directory, '**', '*.msgflow'), recursive=True)
    print(f"Found {len(msgflow_files)} .msgflow files in {directory}")

    if msgflow_files:
        for file in msgflow_files:
            read_msgflow_file(file)
    else:
        print(f"No .msgflow files found in {directory}")

# Method to read each .msgflow file and process its content
def read_msgflow_file(file_path):
    print(f"\nProcessing msgflow file: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            root = ET.fromstring(content)
            namespaces = extract_namespaces(content)
            namespaces = update_namespaces_with_replacements(namespaces)
            for prefix, uri in namespaces.items():
                ET.register_namespace(prefix, uri)
            # Call the subflow replacement function here
            replace_subflow_nodes(root)
            # Convert the modified root back to a string to use in create_new_msgflow
            modified_content = ET.tostring(root, encoding='utf-8').decode('utf-8')
            eStructuralFeatures_data_accum = []
            property_descriptor_data_accum = []
            attribute_links_data_accum = []

            find_subflow_nodes(root, namespaces, file_path, content,
                               eStructuralFeatures_data_accum,
                               property_descriptor_data_accum,
                               attribute_links_data_accum)
            if eStructuralFeatures_data_accum or property_descriptor_data_accum or attribute_links_data_accum:
                create_new_msgflow(file_path, eStructuralFeatures_data_accum, property_descriptor_data_accum, attribute_links_data_accum, modified_content, content)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

# Method to extract namespaces from the XML content
def extract_namespaces(xml_content):
    namespace_pattern = r'xmlns:([^=]+)="([^"]+)"'
    namespaces = {}
    matches = re.findall(namespace_pattern, xml_content)
    for prefix, uri in matches:
        namespaces[prefix] = uri
    return namespaces

# Method to find subflow nodes and accumulate necessary data for modification
def find_subflow_nodes(root, namespaces, original_file_path, msgflow_content, eStructuralFeatures_accum, property_descriptor_accum, attribute_links_accum):
    subflow_found = False
    for node in root.findall(".//composition/nodes"):
        node_type = node.attrib.get('{http://www.omg.org/XMI}type', '')
        if "subflow" in node_type:
            subflow_found = True
            subflow_namespace = node_type.split(':')[0]
            subflow_file_path = namespaces.get(subflow_namespace)

            if subflow_file_path:
                full_subflow_path = os.path.join('/Users/viniththomas/IBM/ACET12/workspace/STD_MFP', subflow_file_path)
                print(f"Identified subflow: {subflow_file_path}")
                print(f"Full subflow path: {full_subflow_path}")
                subflow_data = read_subflow_file(full_subflow_path)
                if subflow_data is not None:
                    eStructuralFeatures_data = extract_eStructuralFeatures(subflow_data)
                    property_descriptor_data = extract_propertyDescriptors(subflow_data, eStructuralFeatures_data, subflow_namespace)
                    attribute_links_data = extract_attributeLinks(subflow_data, eStructuralFeatures_data, subflow_file_path)

                    eStructuralFeatures_accum.extend(eStructuralFeatures_data)
                    property_descriptor_accum.extend(property_descriptor_data)
                    attribute_links_accum.extend(attribute_links_data)
            else:
                print(f"No namespace URI found for prefix: {subflow_namespace}")
    if not subflow_found:
        print("No subflow nodes found in this msgflow file.")

# Method to read subflow files and return its content as an ElementTree
def read_subflow_file(file_path):
    print(f"Reading subflow file: {file_path}")
    if not os.path.exists(file_path):
        print(f"Subflow file does not exist: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            return ET.fromstring(content)
    except Exception as e:
        print(f"Error reading subflow file {file_path}: {e}")
        return None

# Method to extract eStructuralFeatures from subflow data
def extract_eStructuralFeatures(subflow_data):
    extracted_features = []
    for feature in subflow_data.findall(".//eClassifiers/eStructuralFeatures"):
        extracted_features.append(feature)
    print(f"Extracted {len(extracted_features)} eStructuralFeatures from subflow.")
    return extracted_features

# Method to extract property descriptors from subflow data with increment logic
def extract_propertyDescriptors(subflow_data, eStructuralFeatures_data, subflow_namespace):
    extracted_property_descriptors = []
    match = re.search(r'de_it_eai_(.+)\.subflow', subflow_namespace)
    if match:
        subflow_name = match.group(1)
    else:
        subflow_name = subflow_namespace.split('.')[0]
    group_name_prefix = f"Group.{subflow_name}"

    # Generate a unique group name for each subflow
    group_name_prefix = generate_unique_group_name(group_name_prefix)
    print(f"Using group name prefix: {group_name_prefix}")

    property_descriptor_map = {}
    for pd in subflow_data.iter('propertyDescriptor'):
        described_attr = pd.attrib.get('describedAttribute')
        if described_attr:
            property_descriptor_map[described_attr] = pd

    for feature in eStructuralFeatures_data:
        xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')
        if not xmi_id:
            print("eStructuralFeature without xmi:id found, skipping.")
            continue

        property_descriptor = property_descriptor_map.get(xmi_id)
        if property_descriptor is not None:
            clean_descriptor = clean_propertyDescriptor(copy.deepcopy(property_descriptor), group_name_prefix)
            if clean_descriptor is not None:
                extracted_property_descriptors.append(clean_descriptor)
                print(f"Extracted and cleaned propertyDescriptor for describedAttribute: {xmi_id}")
        else:
            print(f"No propertyDescriptor found for describedAttribute: {xmi_id}")

    print(f"Extracted {len(extracted_property_descriptors)} propertyDescriptors from subflow.")
    return extracted_property_descriptors


def generate_unique_group_name(group_name):
    """
    Generates a unique group name by checking if the group name is already used.
    If it is, appends a number suffix to make it unique.
    """
    if group_name not in group_name_tracker:
        group_name_tracker[group_name] = 1
    else:
        count = group_name_tracker[group_name]
        while f"{group_name}{count}" in group_name_tracker:
            count += 1
        group_name_tracker[group_name] = count + 1
        group_name = f"{group_name}{count}"
    return group_name


# Updated method to clean Property Descriptors with increment logic
def clean_propertyDescriptor(descriptor, group_name_prefix):
    descriptor.attrib['groupName'] = group_name_prefix
    described_attribute = descriptor.attrib.get('describedAttribute', '')

    # Increment the describedAttribute to ensure uniqueness
    if described_attribute:
        descriptor.attrib['describedAttribute'] = increment_value(described_attribute)

    # Clean child descriptors if present
    children_to_remove = [child for child in list(descriptor) if child.tag == 'propertyDescriptor']
    for child in children_to_remove:
        descriptor.remove(child)
        print(f"Removed nested propertyDescriptor from groupName: {group_name_prefix}")

    # Recursively adjust nested propertyDescriptors' group names
    for child in descriptor.findall(".//propertyDescriptor"):
        child.attrib['groupName'] = group_name_prefix
        print(f"Updated nested propertyDescriptor groupName to: {group_name_prefix}")

    return descriptor

# Method to extract attribute links from subflow data with increment logic
def extract_attributeLinks(subflow_data, eStructuralFeatures_data, subflow_namespace):
    extracted_attribute_links = []
    for feature in eStructuralFeatures_data:
        xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')
        if not xmi_id:
            print("eStructuralFeature without xmi:id found for attributeLinks, skipping.")
            continue

        attribute_link = subflow_data.find(f".//attributeLinks[@promotedAttribute='{xmi_id}']")
        if attribute_link is not None:
            # Increment the promotedAttribute value to ensure uniqueness
            promoted_attribute = attribute_link.attrib.get('promotedAttribute')
            incremented_attribute = increment_value(promoted_attribute)
            attribute_link.attrib['promotedAttribute'] = incremented_attribute

            # Update the href attribute to match the subflow structure
            for overridden_attribute in attribute_link.findall("overriddenAttribute"):
                href_value = overridden_attribute.attrib.get('href', '')
                if '#' in href_value:
                    href_property = href_value.split('#')[-1]
                    new_href = f"{subflow_namespace}#{href_property}"
                    overridden_attribute.attrib['href'] = new_href
                    print(f"Updated overriddenAttribute href to: {new_href}")
            extracted_attribute_links.append(attribute_link)
            print(f"Extracted attributeLink for promotedAttribute: {incremented_attribute}")
        else:
            print(f"No attributeLink found for promotedAttribute: {xmi_id}")
    print(f"Extracted {len(extracted_attribute_links)} attributeLinks from subflow.")
    return extracted_attribute_links

# Method to increment name and ID attributes of a feature
def increment_name_and_id(feature):
    name = feature.attrib.get('name')
    if name in name_increment_tracker:
        count = name_increment_tracker[name] + 1
        name_increment_tracker[name] = count
    else:
        count = 0
        name_increment_tracker[name] = count
    # Check recursively until a unique name and ID is found
    while f"{name}{count}" in name_increment_tracker.values():
        count += 1
    feature.attrib['name'] = f"{name}{count}"
    xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')
    if xmi_id:
        feature.attrib['{http://www.omg.org/XMI}id'] = f"{xmi_id}.{count}"
    name_increment_tracker[feature.attrib['name']] = count

# Method to increment attributes in property descriptors to ensure uniqueness
def increment_propertyDescriptor(property_desc):
    described_attribute = property_desc.attrib.get('describedAttribute')
    if described_attribute:
        incremented_value = increment_value(described_attribute)
        property_desc.attrib['describedAttribute'] = incremented_value

        for property_name in property_desc.findall('propertyName'):
            key = property_name.attrib.get('key')
            if key:
                property_name.attrib['key'] = f"{key}.{incremented_value}"
        name_increment_tracker[property_desc.attrib['describedAttribute']] = incremented_value

# Method to increment attributes in attribute links to ensure uniqueness
def increment_attributeLinks(attribute_link):
    promoted_attribute = attribute_link.attrib.get('promotedAttribute')
    if promoted_attribute:
        incremented_value = increment_value(promoted_attribute)
        attribute_link.attrib['promotedAttribute'] = incremented_value
        name_increment_tracker[attribute_link.attrib['promotedAttribute']] = incremented_value

# Increment function to handle unique naming
def increment_value(base_value):
    incremented_value = base_value
    while incremented_value in increment_tracker:
        match = re.match(r"(.+?)(\d*)$", incremented_value)
        if match:
            base, num = match.groups()
            if num:
                incremented_value = f"{base}{int(num) + 1}"
            else:
                incremented_value = f"{base}1"
    increment_tracker[incremented_value] = True
    return incremented_value


def update_ecore_package_content(msgflow_content, json_filepath='replacement_mapping.json'):
    """
    Updates the <ecore:EPackage> attributes in the msgflow content based on the JSON replacement mappings.

    :param msgflow_content: The content of the msgflow as a string.
    :param json_filepath: The path to the JSON file containing replacement mappings.
    :return: The updated msgflow content as a string.
    """
    try:
        # Load the replacement mapping from the JSON file
        with open(json_filepath, 'r') as f:
            replacement_data = json.load(f)

        # Iterate through the replacement configurations and update msgflow content
        for key, replacement_info in replacement_data.items():
            # Iterate through each original and replacement namespace mapping
            for original_namespace, replacement_namespace in replacement_info.items():
                if original_namespace != "Name":  # Ignore the "Name" key
                    # Replace in the <ecore:EPackage> string part of the msgflow_content
                    msgflow_content = msgflow_content.replace(original_namespace, replacement_namespace)

        return msgflow_content

    except Exception as e:
        print(f"Error updating <ecore:EPackage> content: {e}")
        return msgflow_content

def update_namespaces_with_replacements(namespaces):
    """
    Updates the extracted namespaces with the replacements provided in the JSON file.
    This function ensures both keys and values in the namespaces are correctly updated.

    :param namespaces: A dictionary of extracted namespaces.
    :return: The updated namespaces dictionary.
    """
    try:
        # Load the replacement mapping from the JSON file
        with open('replacement_mapping.json', 'r') as f:
            replacement_data = json.load(f)

        # Iterate through the replacement configurations
        for key, replacement_info in replacement_data.items():
            # Replace namespace prefixes
            for original_namespace, replacement_namespace in replacement_info.items():
                if original_namespace in namespaces:
                    # Replace the prefix key in the namespaces dictionary
                    namespaces[replacement_namespace] = namespaces.pop(original_namespace)
                    print(f"Replaced namespace prefix '{original_namespace}' with '{replacement_namespace}'")
                else:
                    # Check and replace the values (URIs) in the namespaces dictionary
                    for prefix, uri in namespaces.items():
                        if uri == original_namespace:
                            namespaces[prefix] = replacement_namespace
                            print(f"Replaced namespace URI '{original_namespace}' with '{replacement_namespace}'")
                            break

        return namespaces
    except Exception as e:
        print(f"Error updating namespaces with replacements: {e}")
        return namespaces

# Main method to create new msgflow with the updated logic
def create_new_msgflow(original_file_path, eStructuralFeatures_data, property_descriptor_data, attribute_links_data,
                       msgflow_content, unmodified_content):
    print(f"Creating new msgflow based on {original_file_path}")

    new_msgflow = ET.fromstring(msgflow_content)

    namespaces = extract_namespaces(msgflow_content)

    namespaces = update_namespaces_with_replacements(namespaces)
    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)

    classifiers = new_msgflow.findall(".//eClassifiers")

    for classifier in classifiers:
        existing_features = classifier.findall("eStructuralFeatures")
        if existing_features:
            last_feature = existing_features[-1]
            last_index = list(classifier).index(last_feature)
            print(f"Inserting {len(eStructuralFeatures_data)} eStructuralFeatures after index {last_index}")
            for feature in eStructuralFeatures_data:
                increment_name_and_id(feature)
                classifier.insert(last_index + 1, feature)
                last_index += 1
                print(f"Inserted eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")
        else:
            eSuperTypes_elem = classifier.find("eSuperTypes")
            if eSuperTypes_elem is not None:
                index = list(classifier).index(eSuperTypes_elem)
                print(f"Inserting eStructuralFeatures after eSuperTypes at index {index}")
                for feature in eStructuralFeatures_data:
                    increment_name_and_id(feature)
                    classifier.insert(index + 1, feature)
                    index += 1
                    print(f"Inserted eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")
            else:
                print("No eSuperTypes found, appending eStructuralFeatures at the end.")
                for feature in eStructuralFeatures_data:
                    increment_name_and_id(feature)
                    classifier.append(feature)
                    print(f"Appended eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")

        property_organizer = classifier.find(".//propertyOrganizer")
        if property_organizer is None:
            property_organizer = ET.SubElement(classifier, "propertyOrganizer")
            print("Created new propertyOrganizer element.")

        for property_desc in property_descriptor_data:
            increment_propertyDescriptor(property_desc)
            insert_propertyDescriptor(property_organizer, property_desc)

        for attribute_link in attribute_links_data:
            increment_attributeLinks(attribute_link)
            classifier.append(attribute_link)
            print("Appended attributeLink to eClassifiers.")

    shift_nodes_x_axis(new_msgflow, 200)

    new_file_path = original_file_path.replace(".msgflow", "_new.msgflow")
    print(f"Saving new msgflow to: {new_file_path}")

    try:
        xml_string = ET.tostring(new_msgflow, encoding='utf-8').decode('utf-8')
        start_index = unmodified_content.find('<ecore:EPackage') + len('<ecore:EPackage')
        end_index = unmodified_content.find('>', start_index)
        original_namespace_declaration = unmodified_content[start_index:end_index + 1]

        start_tag_index = xml_string.find('<ecore:EPackage') + len('<ecore:EPackage')
        end_tag_index = xml_string.find('>', start_tag_index)
        new_string = xml_string[:start_tag_index] + original_namespace_declaration + xml_string[end_tag_index + 1:]

        xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
        final_content = xml_declaration + new_string
        updated_msgflow_content = update_ecore_package_content(final_content)

        with open(new_file_path, 'w', encoding='utf-8') as file:
            file.write(updated_msgflow_content)
        print(f"Successfully created new msgflow at: {new_file_path}")
    except Exception as e:
        print(f"Error writing new msgflow: {e}")

# Method to shift the X-axis position of nodes in the msgflow
def shift_nodes_x_axis(root, shift_value):
    try:
        for node in root.findall(".//composition/nodes"):
            location = node.attrib.get('location', '')
            if location:
                x, y = map(int, location.split(','))
                new_location = f"{x + shift_value},{y}"
                node.set('location', new_location)
                print(f"Updated node '{node.attrib.get('xmi:id')}' location to: {new_location}")
    except Exception as e:
        print(f"Error shifting node locations: {e}")

# Method to insert property descriptors into the propertyOrganizer
def insert_propertyDescriptor(property_organizer, new_property_desc):
    current = property_organizer
    while True:
        last_pd = current.findall("propertyDescriptor")
        if not last_pd:
            break
        last_pd = last_pd[-1]
        current = last_pd
    current.append(new_property_desc)
    print(f"Inserted propertyDescriptor with describedAttribute: {new_property_desc.attrib.get('describedAttribute')}")

# Specify your directory path for .msgflow files
directory_path = '/Users/viniththomas/IBM/ACET12/workspace/LOGGING'
find_and_read_msgflow_files(directory_path)
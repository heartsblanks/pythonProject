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
described_attribute_tracker = {}


def find_and_read_msgflow_files(directory):
    msgflow_files = glob.glob(os.path.join(directory, '**', '*.msgflow'), recursive=True)
    print(f"Found {len(msgflow_files)} .msgflow files in {directory}")

    if msgflow_files:
        for file in msgflow_files:
            read_msgflow_file(file)
    else:
        print(f"No .msgflow files found in {directory}")


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
                create_new_msgflow(file_path, eStructuralFeatures_data_accum, property_descriptor_data_accum,
                                   attribute_links_data_accum, modified_content, content)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")


def extract_namespaces(xml_content):
    namespace_pattern = r'xmlns:([^=]+)="([^"]+)"'
    namespaces = {}
    matches = re.findall(namespace_pattern, xml_content)
    for prefix, uri in matches:
        namespaces[prefix] = uri
    return namespaces


# Modify the find_subflow_nodes function to call process_subflow_data
def find_subflow_nodes(root, namespaces, original_file_path, msgflow_content, eStructuralFeatures_accum,
                       property_descriptor_accum, attribute_links_accum):
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
                    # Use process_subflow_data to get incremented data
                    eStructuralFeatures_data, property_descriptor_data, attribute_links_data = process_subflow_data(
                        subflow_data, subflow_namespace, subflow_file_path)

                    # Accumulate the processed data
                    eStructuralFeatures_accum.extend(eStructuralFeatures_data)
                    property_descriptor_accum.extend(property_descriptor_data)
                    attribute_links_accum.extend(attribute_links_data)
            else:
                print(f"No namespace URI found for prefix: {subflow_namespace}")
    if not subflow_found:
        print("No subflow nodes found in this msgflow file.")


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


def process_subflow_data(subflow_data, subflow_namespace, subflow_file_path):
    """
    Process eStructuralFeatures, propertyDescriptor, and attributeLinks from subflow data,
    and perform incrementing of attributes as needed.
    """
    eStructuralFeatures_data = extract_eStructuralFeatures(subflow_data)
    property_descriptor_data = extract_propertyDescriptors(subflow_data, eStructuralFeatures_data, subflow_namespace)
    attribute_links_data = extract_attributeLinks(subflow_data, eStructuralFeatures_data, subflow_file_path)

    # For each eStructuralFeature, increment and update related elements
    for feature in eStructuralFeatures_data:
        increment_name_and_id(feature, property_descriptor_data, attribute_links_data)

    return eStructuralFeatures_data, property_descriptor_data, attribute_links_data


def extract_eStructuralFeatures(subflow_data):
    extracted_features = []
    for feature in subflow_data.findall(".//eClassifiers/eStructuralFeatures"):
        extracted_features.append(feature)
    print(f"Extracted {len(extracted_features)} eStructuralFeatures from subflow.")
    return extracted_features


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
    # Set groupName directly without incrementing
    descriptor.attrib['groupName'] = group_name_prefix

    # Remove child propertyDescriptor elements to flatten the structure
    children_to_remove = [child for child in list(descriptor) if child.tag == 'propertyDescriptor']
    for child in children_to_remove:
        descriptor.remove(child)
        print(f"Removed nested propertyDescriptor from groupName: {group_name_prefix}")

    # Ensure nested propertyDescriptors have the correct groupName
    for child in descriptor.findall(".//propertyDescriptor"):
        child.attrib['groupName'] = group_name_prefix
        print(f"Updated nested propertyDescriptor groupName to: {group_name_prefix}")

    return descriptor


def extract_attributeLinks(subflow_data, eStructuralFeatures_data, subflow_namespace):
    extracted_attribute_links = []
    for feature in eStructuralFeatures_data:
        xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')
        if not xmi_id:
            print("eStructuralFeature without xmi:id found for attributeLinks, skipping.")
            continue

        # Extract attributeLink matching the xmi:id
        attribute_link = subflow_data.find(f".//attributeLinks[@promotedAttribute='{xmi_id}']")
        if attribute_link is not None:
            # Only update href without incrementing promotedAttribute
            for overridden_attribute in attribute_link.findall("overriddenAttribute"):
                href_value = overridden_attribute.attrib.get('href', '')
                if '#' in href_value:
                    href_property = href_value.split('#')[-1]
                    new_href = f"{subflow_namespace}#{href_property}"
                    overridden_attribute.attrib['href'] = new_href
                    print(f"Updated overriddenAttribute href to: {new_href}")
            extracted_attribute_links.append(attribute_link)
            print(f"Extracted attributeLink for promotedAttribute: {xmi_id}")
        else:
            print(f"No attributeLink found for promotedAttribute: {xmi_id}")
    print(f"Extracted {len(extracted_attribute_links)} attributeLinks from subflow.")
    return extracted_attribute_links

# Method to increment name and ID attributes of a feature
def increment_name_and_id(feature, property_descriptor_data, attribute_links_data):
    """
    Increments the name and ID of eStructuralFeatures and updates corresponding
    propertyDescriptor and attributeLink entries if the count is greater than 0.
    """
    name = feature.attrib.get('name')
    xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')

    # Separate base name and numeric suffix
    match = re.match(r"(.+?)(\d*)$", name)
    if match:
        base_name, num = match.groups()
        num = int(num) if num else 0
    else:
        base_name, num = name, 0

    # Initialize count in tracker if not present
    if base_name not in name_increment_tracker:
        name_increment_tracker[base_name] = 0
    else:
        name_increment_tracker[base_name] += 1

    # Get current count from tracker
    current_count = name_increment_tracker[base_name]

    # Only increment if current_count is greater than 0
    if current_count > 0:
        feature.attrib['name'] = f"{base_name}{current_count}"
        if xmi_id:
            feature.attrib['{http://www.omg.org/XMI}id'] = f"Property.{base_name}{current_count}"
            modified_id = f"Property.{base_name}{current_count}"



            # Update related propertyDescriptor and attributeLink elements using this count
        update_related_elements(xmi_id, current_count, property_descriptor_data, attribute_links_data, modified_id)


def update_related_elements(original_id, count, property_descriptor_data, attribute_links_data, modified_id):
    """
    Updates propertyDescriptor and attributeLink elements with the incremented count.
    """
    # Update propertyDescriptor entries
    for prop_desc in property_descriptor_data:
        described_attribute = prop_desc.attrib.get('describedAttribute')
        if described_attribute == original_id:
            # Increment describedAttribute and related propertyName key
            prop_desc.attrib['describedAttribute'] = f"{modified_id}"
            for property_name in prop_desc.findall('propertyName'):
                key = property_name.attrib.get('key')
                if key:
                    property_name.attrib['key'] = f"{modified_id}"
            print(f"Updated propertyDescriptor for describedAttribute: {prop_desc.attrib['describedAttribute']}")

    # Update attributeLink entries
    for attr_link in attribute_links_data:
        promoted_attribute = attr_link.attrib.get('promotedAttribute')
        if promoted_attribute == original_id:
            # Increment promotedAttribute
            attr_link.attrib['promotedAttribute'] = f"{modified_id}"
            for overridden_attribute in attr_link.findall('overriddenAttribute'):
                href = overridden_attribute.get('href')
                href_first_part = href.split('#')[0]
                overridden_attribute.attrib['href'] = f"{href_first_part}#{original_id}"
            print(f"Updated attributeLink for promotedAttribute: {attr_link.attrib['promotedAttribute']}")

# Method to increment attributes in property descriptors to ensure uniqueness



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
    """
    Creates a new msgflow file based on the original content and the accumulated eStructuralFeatures,
    propertyDescriptors, and attributeLinks data.
    """
    print(f"Creating new msgflow based on {original_file_path}")

    # Parse the modified msgflow content into an XML tree
    new_msgflow = ET.fromstring(msgflow_content)

    # Extract and update namespaces as needed
    namespaces = extract_namespaces(msgflow_content)
    namespaces = update_namespaces_with_replacements(namespaces)
    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)

    # Find all eClassifiers in the msgflow
    classifiers = new_msgflow.findall(".//eClassifiers")

    for classifier in classifiers:
        # Directly insert the eStructuralFeatures data
        existing_features = classifier.findall("eStructuralFeatures")
        if existing_features:
            # Insert after the last existing feature
            last_feature = existing_features[-1]
            last_index = list(classifier).index(last_feature)
            print(f"Inserting {len(eStructuralFeatures_data)} eStructuralFeatures after index {last_index}")
            for feature in eStructuralFeatures_data:
                # No need to increment as it's already processed
                classifier.insert(last_index + 1, feature)
                last_index += 1
                print(f"Inserted eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")
        else:
            # If no existing eStructuralFeatures, insert after eSuperTypes or at the end
            eSuperTypes_elem = classifier.find("eSuperTypes")
            if eSuperTypes_elem is not None:
                index = list(classifier).index(eSuperTypes_elem)
                print(f"Inserting eStructuralFeatures after eSuperTypes at index {index}")
                for feature in eStructuralFeatures_data:
                    classifier.insert(index + 1, feature)
                    index += 1
                    print(f"Inserted eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")
            else:
                # Append at the end if no eSuperTypes found
                print("No eSuperTypes found, appending eStructuralFeatures at the end.")
                for feature in eStructuralFeatures_data:
                    classifier.append(feature)
                    print(f"Appended eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")

        # Handle propertyDescriptor insertion inside propertyOrganizer
        property_organizer = classifier.find(".//propertyOrganizer")
        if property_organizer is None:
            property_organizer = ET.SubElement(classifier, "propertyOrganizer")
            print("Created new propertyOrganizer element.")

        for property_desc in property_descriptor_data:
            # Directly insert without incrementing
            insert_propertyDescriptor(property_organizer, property_desc)

        # Handle attributeLink insertion
        for attribute_link in attribute_links_data:
            # Directly append attributeLinks
            classifier.append(attribute_link)
            print("Appended attributeLink to eClassifiers.")

    # Shift the X-axis position of nodes to avoid overlap (if required)
    shift_nodes_x_axis(new_msgflow, 200)

    # Save the new msgflow file
    new_file_path = original_file_path.replace(".msgflow", "_new.msgflow")
    print(f"Saving new msgflow to: {new_file_path}")

    try:
        # Convert the modified ElementTree to a string
        xml_string = ET.tostring(new_msgflow, encoding='utf-8').decode('utf-8')

        # Retain the original namespace declarations for <ecore:EPackage>
        start_index = unmodified_content.find('<ecore:EPackage') + len('<ecore:EPackage')
        end_index = unmodified_content.find('>', start_index)
        original_namespace_declaration = unmodified_content[start_index:end_index + 1]

        # Replace the <ecore:EPackage> tag with the original namespace declaration
        start_tag_index = xml_string.find('<ecore:EPackage') + len('<ecore:EPackage')
        end_tag_index = xml_string.find('>', start_tag_index)
        new_string = xml_string[:start_tag_index] + original_namespace_declaration + xml_string[end_tag_index + 1:]

        # Add the correct XML declaration at the start
        xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
        final_content = xml_declaration + new_string

        # Update <ecore:EPackage> with replacements before saving the file
        updated_msgflow_content = update_ecore_package_content(final_content)

        # Write the final content to the file
        with open(new_file_path, 'w', encoding='utf-8') as file:
            file.write(updated_msgflow_content)
        print(f"Successfully created new msgflow at: {new_file_path}")
    except Exception as e:
        print(f"Error writing new msgflow: {e}")


def insert_propertyDescriptor(property_organizer, new_property_desc):
    """
    Inserts the new_property_desc into the propertyOrganizer element.
    """
    current = property_organizer
    while True:
        last_pd = current.findall("propertyDescriptor")
        if not last_pd:
            break
        last_pd = last_pd[-1]
        current = last_pd
    current.append(new_property_desc)
    print(f"Inserted propertyDescriptor with describedAttribute: {new_property_desc.attrib.get('describedAttribute')}")


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
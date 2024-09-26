def increment_subflow_elements(eStructuralFeatures, propertyDescriptors, attributeLinks):
    """
    Increment values for eStructuralFeatures, propertyDescriptors, and attributeLinks together.

    :param eStructuralFeatures: List of eStructuralFeatures elements.
    :param propertyDescriptors: List of propertyDescriptor elements.
    :param attributeLinks: List of attributeLink elements.
    """
    for feature in eStructuralFeatures:
        name = feature.attrib.get('name')
        xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')

        # Check if the name already exists in the tracker
        if name in name_increment_tracker:
            # Increment name and ID if already exists
            incremented_name = increment_value(name)
            incremented_id = increment_value(xmi_id)

            # Update eStructuralFeature with incremented name and ID
            feature.attrib['name'] = incremented_name
            feature.attrib['{http://www.omg.org/XMI}id'] = incremented_id

            # Update corresponding propertyDescriptor
            for property_desc in propertyDescriptors:
                if property_desc.attrib.get('describedAttribute') == xmi_id:
                    property_desc.attrib['describedAttribute'] = incremented_id
                    for property_name in property_desc.findall('propertyName'):
                        key = property_name.attrib.get('key')
                        if key:
                            property_name.attrib['key'] = increment_value(key)

            # Update corresponding attributeLink
            for attribute_link in attributeLinks:
                if attribute_link.attrib.get('promotedAttribute') == xmi_id:
                    attribute_link.attrib['promotedAttribute'] = incremented_id

            # Store the updated name in the tracker to avoid future conflicts
            name_increment_tracker[incremented_name] = True
        else:
            # If no increment is required, store the original name in the tracker
            name_increment_tracker[name] = True
            
def increment_value(base_value):
    """
    Increment a base value until it is unique in the name_increment_tracker.
    """
    incremented_value = base_value
    while incremented_value in name_increment_tracker:
        match = re.match(r"(.+?)(\d*)$", incremented_value)
        if match:
            base, num = match.groups()
            if num:
                incremented_value = f"{base}{int(num) + 1}"
            else:
                incremented_value = f"{base}1"
    name_increment_tracker[incremented_value] = True  # Track the new unique value
    return incremented_value
    
def extract_subflow_data(subflow_data):
    eStructuralFeatures = []
    propertyDescriptors = []
    attributeLinks = []

    # Extract eStructuralFeatures
    eStructuralFeatures.extend(subflow_data.findall(".//eClassifiers/eStructuralFeatures"))

    # Extract propertyDescriptors
    property_descriptor_map = {}
    for pd in subflow_data.iter('propertyDescriptor'):
        described_attr = pd.attrib.get('describedAttribute')
        if described_attr:
            property_descriptor_map[described_attr] = pd
    propertyDescriptors.extend(property_descriptor_map.values())

    # Extract attributeLinks
    for feature in eStructuralFeatures:
        xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')
        if not xmi_id:
            continue
        attribute_link = subflow_data.find(f".//attributeLinks[@promotedAttribute='{xmi_id}']")
        if attribute_link is not None:
            attributeLinks.append(attribute_link)

    return eStructuralFeatures, propertyDescriptors, attributeLinks

def process_subflow_data(subflow_data):
    """
    Process subflow data to extract and increment elements.
    :param subflow_data: XML ElementTree of the subflow data.
    :return: Incremented eStructuralFeatures, propertyDescriptors, and attributeLinks.
    """
    # Step 1: Extract data without modifying
    eStructuralFeatures, propertyDescriptors, attributeLinks = extract_subflow_data(subflow_data)

    # Step 2: Increment elements together
    increment_subflow_elements(eStructuralFeatures, propertyDescriptors, attributeLinks)

    # Step 3: Return incremented data
    return eStructuralFeatures, propertyDescriptors, attributeLinks
    
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
                    # Step 1 and Step 2: Process and increment subflow elements together
                    eStructuralFeatures_data, property_descriptor_data, attribute_links_data = process_subflow_data(subflow_data)
                    
                    # Step 3: Accumulate processed data
                    eStructuralFeatures_accum.extend(eStructuralFeatures_data)
                    property_descriptor_accum.extend(property_descriptor_data)
                    attribute_links_accum.extend(attribute_links_data)
            else:
                print(f"No namespace URI found for prefix: {subflow_namespace}")
    if not subflow_found:
        print("No subflow nodes found in this msgflow file.")
        
def create_new_msgflow(original_file_path, eStructuralFeatures_data, property_descriptor_data, attribute_links_data, msgflow_content):
    print(f"Creating new msgflow based on {original_file_path}")

    new_msgflow = ET.fromstring(msgflow_content)
    namespaces = extract_namespaces(msgflow_content)
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
                # No need to increment again, use already processed data
                classifier.insert(last_index + 1, feature)
                last_index += 1
                print(f"Inserted eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")
        else:
            eSuperTypes_elem = classifier.find("eSuperTypes")
            if eSuperTypes_elem is not None:
                index = list(classifier).index(eSuperTypes_elem)
                print(f"Inserting eStructuralFeatures after eSuperTypes at index {index}")
                for feature in eStructuralFeatures_data:
                    classifier.insert(index + 1, feature)
                    index += 1
                    print(f"Inserted eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")
            else:
                print("No eSuperTypes found, appending eStructuralFeatures at the end.")
                for feature in eStructuralFeatures_data:
                    classifier.append(feature)
                    print(f"Appended eStructuralFeature with xmi:id: {feature.attrib.get('xmi:id')}")

        property_organizer = classifier.find(".//propertyOrganizer")
        if property_organizer is None:
            property_organizer = ET.SubElement(classifier, "propertyOrganizer")
            print("Created new propertyOrganizer element.")

        for property_desc in property_descriptor_data:
            # No need to increment again, use already processed data
            insert_propertyDescriptor(property_organizer, property_desc)

        for attribute_link in attribute_links_data:
            # No need to increment again, use already processed data
            classifier.append(attribute_link)
            print("Appended attributeLink to eClassifiers.")

    new_file_path = original_file_path.replace(".msgflow", "_new.msgflow")
    print(f"Saving new msgflow to: {new_file_path}")

    try:
        xml_string = ET.tostring(new_msgflow, encoding='utf-8').decode('utf-8')
        start_index = msgflow_content.find('<ecore:EPackage') + len('<ecore:EPackage')
        end_index = msgflow_content.find('>', start_index)
        original_namespace_declaration = msgflow_content[start_index:end_index + 1]
        start_tag_index = xml_string.find('<ecore:EPackage') + len('<ecore:EPackage')
        end_tag_index = xml_string.find('>', start_tag_index)
        new_string = xml_string[:start_tag_index] + original_namespace_declaration + xml_string[end_tag_index + 1:]
        xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>\n'
        final_content = xml_declaration + new_string

        with open(new_file_path, 'w', encoding='utf-8') as file:
            file.write(final_content)
        print(f"Successfully created new msgflow at: {new_file_path}")
    except Exception as e:
        print(f"Error writing new msgflow: {e}")
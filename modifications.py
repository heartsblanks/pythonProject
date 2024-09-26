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
def increment_name_and_id(feature):
    name = feature.attrib.get('name')
    if name in name_increment_tracker:
        count = name_increment_tracker[name] + 1
    else:
        # Check if the name already exists in the tracker
        count = 0
        while f"{name}{count}" in name_increment_tracker.values():
            count += 1
        name_increment_tracker[name] = count

    # Increment the name if necessary
    feature.attrib['name'] = f"{name}{count}"

    xmi_id = feature.attrib.get('{http://www.omg.org/XMI}id')
    if xmi_id:
        # Increment the ID if necessary
        feature.attrib['{http://www.omg.org/XMI}id'] = f"{xmi_id}.{count}"

    # Track the new unique name and ID
    name_increment_tracker[feature.attrib['name']] = count
    
    
    def increment_propertyDescriptor(property_desc):
    described_attribute = property_desc.attrib.get('describedAttribute')
    if described_attribute:
        count = 0
        # Check recursively until a unique attribute is found
        while f"{described_attribute}.{count}" in name_increment_tracker.values():
            count += 1

        if count > 0:  # Increment only if necessary
            property_desc.attrib['describedAttribute'] = f"{described_attribute}.{count}"
            for property_name in property_desc.findall('propertyName'):
                key = property_name.attrib.get('key')
                if key:
                    property_name.attrib['key'] = f"{key}.{count}"

        name_increment_tracker[property_desc.attrib['describedAttribute']] = count
        
        
        def increment_attributeLinks(attribute_link):
    promoted_attribute = attribute_link.attrib.get('promotedAttribute')
    if promoted_attribute:
        count = 0
        # Check recursively until a unique attribute is found
        while f"{promoted_attribute}.{count}" in name_increment_tracker.values():
            count += 1

        if count > 0:  # Increment only if necessary
            attribute_link.attrib['promotedAttribute'] = f"{promoted_attribute}.{count}"

        name_increment_tracker[attribute_link.attrib['promotedAttribute']] = count
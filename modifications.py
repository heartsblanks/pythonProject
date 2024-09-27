import xml.etree.ElementTree as ET

def add_input_node_correct(msgflow_content, input_node_data):
    """
    Adds an input node to the msgflow content by appending it to the nodes section within the composition.

    :param msgflow_content: The XML content of the msgflow as a string.
    :param input_node_data: A dictionary containing the input node details like xmi:type, xmi:id, location, etc.
    :return: Modified msgflow content with the new input node added.
    """
    try:
        # Parse the msgflow content into an XML tree
        root = ET.fromstring(msgflow_content)
        
        # Find the composition section
        composition_section = root.find(".//composition")
        if composition_section is None:
            print("Composition section not found in msgflow content.")
            return msgflow_content

        # Create a new node element using the provided input_node_data
        new_node = ET.Element("nodes", {
            "xmi:type": input_node_data["xmi:type"],
            "xmi:id": input_node_data["xmi:id"],
            "location": input_node_data["location"]
        })

        # Add translation sub-element if provided
        if "translation" in input_node_data:
            translation_element = ET.SubElement(new_node, "translation", {
                "xmi:type": "utility:ConstantString",
                "string": input_node_data["translation"]
            })
        
        # Find the position to insert the new node before any connections
        inserted = False
        for i, child in enumerate(composition_section):
            if child.tag == 'connections':
                # Insert the new node before the first connections element
                composition_section.insert(i, new_node)
                inserted = True
                break
        
        # If no connections found, append to the end of composition section
        if not inserted:
            composition_section.append(new_node)
        
        # Convert the modified XML tree back to a string
        return ET.tostring(root, encoding='utf-8').decode('utf-8')
        
    except ET.ParseError as e:
        print(f"Error parsing msgflow content: {e}")
        return msgflow_content


# Example usage
msgflow_content = '''<your_xml_content_here>'''  # Replace with your actual msgflow content
input_node_data = {
    "xmi:type": "ComIbmMQInput.msgnode:FCMComposite_1",
    "xmi:id": "FCMComposite_1_4",  # Replace with the desired xmi:id
    "location": "12,222",
    "translation": "MQ Input"
}

modified_content = add_input_node_correct(msgflow_content, input_node_data)
print(modified_content)


import xml.etree.ElementTree as ET

def add_connection_as_last_child(msgflow_content, connection_data):
    """
    Adds a new connection as the last child of the composition section in the msgflow content.

    :param msgflow_content: The XML content of the msgflow as a string.
    :param connection_data: A dictionary containing the connection details like xmi:type, xmi:id, targetNode, sourceNode, etc.
    :return: Modified msgflow content with the new connection added.
    """
    try:
        # Parse the msgflow content into an XML tree
        root = ET.fromstring(msgflow_content)
        
        # Find the composition section
        composition_section = root.find(".//composition")
        if composition_section is None:
            print("Composition section not found in msgflow content.")
            return msgflow_content

        # Create a new connection element using the provided connection_data
        new_connection = ET.Element("connections", {
            "xmi:type": connection_data["xmi:type"],
            "xmi:id": connection_data["xmi:id"],
            "targetNode": connection_data["targetNode"],
            "sourceNode": connection_data["sourceNode"],
            "sourceTerminalName": connection_data["sourceTerminalName"],
            "targetTerminalName": connection_data["targetTerminalName"]
        })
        
        # Append the new connection as the last child of the composition section
        composition_section.append(new_connection)
        
        # Convert the modified XML tree back to a string
        return ET.tostring(root, encoding='utf-8').decode('utf-8')
        
    except ET.ParseError as e:
        print(f"Error parsing msgflow content: {e}")
        return msgflow_content


# Example usage
msgflow_content = '''<your_xml_content_here>'''  # Replace with your actual msgflow content
connection_data = {
    "xmi:type": "eflow:FCMConnection",
    "xmi:id": "FCMConnection_8",  # Replace with the desired xmi:id
    "targetNode": "FCMComposite_1_2",  # Replace with the desired target node ID
    "sourceNode": "FCMComposite_1_4",  # Replace with the desired source node ID
    "sourceTerminalName": "OutTerminal.out",
    "targetTerminalName": "InTerminal.Input1"
}

modified_content = add_connection_as_last_child(msgflow_content, connection_data)
print(modified_content)
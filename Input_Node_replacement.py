import json
import re
import xml.etree.ElementTree as ET

def replace_subflow_nodes(new_msgflow, json_filepath='replacement_mapping.json'):
    """
    Replaces specified subflow nodes in the msgflow based on the mappings defined in a JSON file.

    :param new_msgflow: The root element of the msgflow XML tree.
    :param json_filepath: The path to the JSON file containing the replacement mappings.
    """
    # Load the replacement mapping from a JSON file
    with open(json_filepath, 'r') as f:
        replacement_data = json.load(f)

    # Iterate through all nodes in the composition section of the msgflow
    for node in new_msgflow.findall(".//composition/nodes"):
        node_type = node.attrib.get('{http://www.omg.org/XMI}type', '')

        # Check if any key in replacement_data exists within node_type
        for key, replacement_info in replacement_data.items():
            if key in node_type:
                replacement_name = replacement_info.get("Name", "")

                # Iterate through each original and replacement namespace mapping
                for original_namespace, replacement_namespace in replacement_info.items():
                    if original_namespace in ["Name"]:
                        continue

                    # Replace the node type if it contains the original namespace
                    if original_namespace in node_type:
                        node.attrib['{http://www.omg.org/XMI}type'] = node_type.replace(original_namespace,
                                                                                        replacement_namespace)

                        # Update translation string if present
                        translation_node = node.find(".//translation")
                        if translation_node is not None:
                            translation_node.set('string', replacement_name)

                        print(f"Replaced subflow node '{node_type}' with '{replacement_namespace}'")
                        break

def add_input_node_to_msgflow(msgflow_root, namespaces, json_filepath='input_node_config.json'):
    """
    Adds an input node to the msgflow based on details from a JSON configuration file.

    :param msgflow_root: The root element of the msgflow XML.
    :param json_filepath: The path to the JSON file containing node and connection details.
    """
    # Load input node configuration from the JSON file
    with open(json_filepath, 'r') as f:
        input_node_config = json.load(f)

    input_node_details = input_node_config.get("input_node", {})
    namespace = input_node_details.get("namespace")
    node_config = input_node_details.get("node", {})
    connections_config = input_node_details.get("connections", [])

    # Update the ecore:EPackage element with the namespace if not present
    ecore_package = msgflow_root.find(".//ecore:EPackage", namespaces)
    if ecore_package is not None and namespace not in ecore_package.attrib:
        ecore_package.attrib[f"xmlns:{namespace}"] = namespace
        print(f"Added namespace '{namespace}' to ecore:EPackage.")

    # Get the maximum xmi:id for nodes and connections
    max_node_id = get_max_xmi_id(msgflow_root, ".//composition/nodes", "FCMComposite")
    max_connection_id = get_max_xmi_id(msgflow_root, ".//connections", "FCMConnection")

    # Add the new node element
    new_node_id = f"FCMComposite_{max_node_id + 1}"
    new_node = ET.Element("nodes", {
        "xmi:type": node_config.get("xmi_type", ""),
        "xmi:id": new_node_id,
        "location": node_config.get("location", "")
    })
    translation_element = ET.SubElement(new_node, "translation", {
        "xmi:type": "utility:ConstantString",
        "string": node_config.get("translation", "")
    })
    print(f"Added new node with ID '{new_node_id}'.")

    # Insert the new node into the nodes section
    nodes_section = msgflow_root.find(".//composition")
    nodes_section.append(new_node)

    # Add connection elements based on the provided configuration
    for connection_details in connections_config:
        max_connection_id += 1
        connection_id = f"FCMConnection_{max_connection_id}"
        new_connection = ET.Element("connections", {
            "xmi:type": "eflow:FCMConnection",
            "xmi:id": connection_id,
            "sourceNode": new_node_id,
            "targetNode": "FCMComposite_1_2",  # This should be dynamically set based on your logic
            "sourceTerminalName": connection_details.get("sourceTerminalName"),
            "targetTerminalName": connection_details.get("targetTerminalName")
        })
        nodes_section.append(new_connection)
        print(f"Added new connection with ID '{connection_id}'.")

def get_max_xmi_id(msgflow_root, xpath, prefix):
    """
    Returns the maximum xmi:id value for elements matching the given XPath and prefix.

    :param msgflow_root: The root element of the msgflow XML.
    :param xpath: The XPath expression to select elements.
    :param prefix: The prefix to match for xmi:id values (e.g., "FCMComposite").
    :return: The maximum numerical suffix of the xmi:id values.
    """
    max_id = 0
    for elem in msgflow_root.findall(xpath):
        xmi_id = elem.attrib.get("{http://www.omg.org/XMI}id", "")
        match = re.match(fr"{prefix}_(\d+)", xmi_id)
        if match:
            num = int(match.group(1))
            if num > max_id:
                max_id = num
    return max_id
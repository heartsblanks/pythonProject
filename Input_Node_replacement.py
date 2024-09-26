import json


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
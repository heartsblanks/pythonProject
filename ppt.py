import os
import re
from pptx import Presentation

def extract_info_from_files(folder_path):
    # Define regex patterns for PAP, PF, queues, and tables
    pap_pattern = r"(PAP\w+)"
    pf_pattern = r"(PF\w+)"
    queue_pattern = r"(\w+_(EVT|CPY|ERR))"
    tables_pattern = r"(tables.*?)(\w+)"

    results = []

    # Iterate over each file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".ppt") or filename.endswith(".pptx"):
            file_data = {
                "filename": filename,
                "PAP_matches": re.findall(pap_pattern, filename),
                "PF_matches": re.findall(pf_pattern, filename),
                "Queue_matches": re.findall(queue_pattern, filename),
                "Table_names": []
            }

            # Load the PowerPoint file to search for tables inside slides
            ppt_path = os.path.join(folder_path, filename)
            presentation = Presentation(ppt_path)

            # Search through each slide for text containing "tables" and follow-up names
            for slide in presentation.slides:
                for shape in slide.shapes:
                    if shape.has_text_frame:
                        text = shape.text_frame.text
                        table_matches = re.findall(tables_pattern, text, re.IGNORECASE)
                        if table_matches:
                            # Append matched table names (only names after 'tables')
                            file_data["Table_names"].extend([match[1] for match in table_matches])

            results.append(file_data)

    # Display results
    for result in results:
        print(f"File: {result['filename']}")
        print(f"  PAP Matches: {', '.join(result['PAP_matches']) if result['PAP_matches'] else 'None'}")
        print(f"  PF Matches: {', '.join(result['PF_matches']) if result['PF_matches'] else 'None'}")
        print(f"  Queue Matches: {', '.join([q[0] for q in result['Queue_matches']]) if result['Queue_matches'] else 'None'}")
        print(f"  Table Names: {', '.join(result['Table_names']) if result['Table_names'] else 'None'}\n")

# Usage example
folder_path = "/path/to/your/ppt/folder"  # Replace with your folder path
extract_info_from_files(folder_path)
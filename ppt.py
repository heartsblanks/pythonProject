import os
import re
from pptx import Presentation
import comtypes.client

def convert_ppt_to_pptx(folder_path):
    powerpoint = comtypes.client.CreateObject("PowerPoint.Application")
    powerpoint.Visible = 1

    for filename in os.listdir(folder_path):
        if filename.endswith(".ppt"):
            full_path = os.path.join(folder_path, filename)
            pptx_path = os.path.join(folder_path, filename.replace(".ppt", ".pptx"))

            try:
                # Open and save as .pptx
                presentation = powerpoint.Presentations.Open(full_path)
                presentation.SaveAs(pptx_path, 24)  # 24 is the format ID for .pptx
                presentation.Close()
                
                # Delete the original .ppt file after successful conversion
                os.remove(full_path)
                print(f"Converted {filename} to .pptx and removed the original .ppt file.")
            except Exception as e:
                print(f"Failed to convert {filename}: {e}")

    powerpoint.Quit()

def extract_content_from_pptx(folder_path):
    # Define regex patterns for PAP, PF, and queue names
    pap_pattern = r"\bPAP\w+"
    pf_pattern = r"\bPF\w+"
    queue_end_pattern = r"\b\w+_(EVT|CPY|ERR)\b"  # Matches end of a queue name with _EVT, _CPY, or _ERR

    results = []

    for filename in os.listdir(folder_path):
        if filename.endswith(".pptx"):
            file_path = os.path.join(folder_path, filename)
            presentation = Presentation(file_path)
            file_data = {
                "filename": filename,
                "PAP_matches": [],
                "PF_matches": [],
                "Queue_matches": []
            }

            # Extract text from each slide and check for matches
            for slide in presentation.slides:
                for shape in slide.shapes:
                    if shape.has_text_frame:
                        lines = shape.text_frame.text.splitlines()
                        queue_name = ""
                        
                        for line in lines:
                            # Accumulate queue name across lines ending with "_"
                            if line.endswith("_"):
                                queue_name += line.strip("_")  # Strip the trailing "_"
                            else:
                                # Append the last line to complete the queue name
                                queue_name += line
                                
                                # Check if the full queue name matches the pattern
                                if re.search(queue_end_pattern, queue_name):
                                    file_data["Queue_matches"].append(queue_name)
                                
                                # Reset the queue name for the next one
                                queue_name = ""

                        # Find matches for PAP and PF patterns
                        text = shape.text_frame.text
                        file_data["PAP_matches"].extend(re.findall(pap_pattern, text))
                        file_data["PF_matches"].extend(re.findall(pf_pattern, text))

            results.append(file_data)

    # Display results
    for result in results:
        print(f"File: {result['filename']}")
        print(f"  PAP Matches: {', '.join(set(result['PAP_matches'])) if result['PAP_matches'] else 'None'}")
        print(f"  PF Matches: {', '.join(set(result['PF_matches'])) if result['PF_matches'] else 'None'}")
        print(f"  Queue Matches: {', '.join(set(result['Queue_matches'])) if result['Queue_matches'] else 'None'}\n")

# Usage example
folder_path = "/path/to/your/ppt/folder"  # Replace with your folder path
convert_ppt_to_pptx(folder_path)  # First, convert .ppt to .pptx if needed
extract_content_from_pptx(folder_path)  # Then, extract text content
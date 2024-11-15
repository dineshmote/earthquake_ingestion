import json
import os

def sample_json_data(file_path):
    """
    Reads a JSON file and returns its content as a formatted JSON string using json.dumps().
    
    Args:
        file_path (str): The path to the JSON file.
        
    Returns:
        str: The content of the JSON file as a formatted JSON string.
    """
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            json_data = json.load(file)  # Load the JSON content into a Python dictionary or list
        json_string = json.dumps(json_data, indent=4)  # Convert the Python object back to a JSON string
        return json_string
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file: {e}")
        return None
    except Exception as e:
        print(f"Error reading the JSON file: {e}")
        return None

# Example usage
file_path = r"C:\Brainworks\earthquake_ingestion\test\sample_data.json"
json_string = sample_json_data(file_path)
if json_string:
    print(json_string)  # Print the formatted JSON string

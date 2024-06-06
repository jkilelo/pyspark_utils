import subprocess

def get_autosys_box_status(box_name):
    try:
        # Run the autorep command to get the status of the Autosys box
        result = subprocess.run(['autorep', '-J', box_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # Check if the command was successful
        if result.returncode != 0:
            print(f"Error retrieving status for {box_name}: {result.stderr}")
            return None
        
        # Parse the output to extract the status
        for line in result.stdout.splitlines():
            if 'Status:' in line:
                return line.split()[-1]  # Assuming the status is the last word in the line

        # If status is not found in the output
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def check_statuses(box_list):
    status_dict = {}
    for box in box_list:
        status = get_autosys_box_status(box)
        status_dict[box] = status
    return status_dict

# Example usage
boxes = [f"box_{i}" for i in range(1, 99)]  # Replace with actual box names

box_statuses = check_statuses(boxes)

print("Box Statuses:")
for box, status in box_statuses.items():
    print(f"{box}: {status}")

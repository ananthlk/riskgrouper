import os
import pandas as pd
from datetime import datetime

def count_lines_of_code(directory):
    """
    Counts the lines of code in all files within a directory, excluding blank lines and comments.
    """
    lines_of_code = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(('.py', '.sql')):
                filepath = os.path.join(root, file)
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    code_lines = [line for line in lines if line.strip() and not line.strip().startswith(('#', '/*', '*/', '--'))]
                    lines_of_code[filepath] = len(code_lines)
    return lines_of_code

def update_code_tracker(tracker_file='code_tracker.csv'):
    """
    Updates a CSV file with the current lines of code for the project.
    """
    directories_to_track = ['src', 'scripts']
    
    all_lines = {}
    for directory in directories_to_track:
        all_lines.update(count_lines_of_code(directory))
        
    total_lines = sum(all_lines.values())
    
    new_entry = {
        'date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        'total_lines_of_code': [total_lines]
    }
    
    if os.path.exists(tracker_file):
        tracker_df = pd.read_csv(tracker_file)
    else:
        tracker_df = pd.DataFrame(columns=['date', 'total_lines_of_code'])
        
    new_df = pd.DataFrame(new_entry)
    tracker_df = pd.concat([tracker_df, new_df], ignore_index=True)
    
    tracker_df.to_csv(tracker_file, index=False)
    print(f"Code tracker updated. Total lines of code: {total_lines}")

if __name__ == '__main__':
    update_code_tracker()

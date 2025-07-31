import csv
import os

def display_csv_summary(file_path, name):
    """Reads a CSV file and displays a summary."""
    print(f"\n--- {name} Summary ---")
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader, None)  # Read header
            rows = list(reader)

            print(f"Total offers: {len(rows)}")
            if len(rows) > 0:
                print("Last 5 offers (or fewer if less than 5):")
                for i, row in enumerate(rows[-5:]):
                    print(f"  {i+1}. {row}")
            else:
                print("No offers found.")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

def display_log_summary(file_path, name, num_lines=10):
    """Reads and displays the last few lines of a log file."""
    print(f"\n--- {name} Log (Last {num_lines} lines) ---")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[-num_lines:]:
                print(line.strip())
    except FileNotFoundError:
        print(f"Log file not found: {file_path}")
    except Exception as e:
        print(f"Error reading log file {file_path}: {e}")

def display_directory_contents(directory_path, name):
    """Lists files in a directory and displays the first two lines of each."""
    print(f"\n--- {name} Directory Contents ---")
    try:
        files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
        if not files:
            print(f"No files found in {directory_path}")
            return

        for file_name in files:
            full_path = os.path.join(directory_path, file_name)
            print(f"\nFile: {file_name}")
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        if i >= 2: # Read only first two lines
                            break
                        print(f"  {line.strip()}")
            except Exception as e:
                print(f"  Could not read file: {e}")

    except FileNotFoundError:
        print(f"Directory not found: {directory_path}")
    except Exception as e:
        print(f"Error listing directory {directory_path}: {e}")


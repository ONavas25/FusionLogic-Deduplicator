
#--------------------------------------------------------------
# Section 1: Import Statements
#--------------------------------------------------------------

import os  # Handles file and directory operations
import json  # Manages JSON serialization and deserialization
from itertools import islice  # Efficient slicing of iterables
from tqdm import tqdm  # Provides a progress bar for loops
from multiprocessing import Pool, cpu_count  # Enables parallel processing
from collections import defaultdict  # Helps structure dictionary-based data operations
from concurrent.futures import ProcessPoolExecutor  # Allows efficient multi-processing execution
import aiofiles  # Async file operations for improved performance
import asyncio  # Supports asynchronous execution in various tasks
import shutil  # Handles file and directory manipulation
import psutil  # Monitors system resource usage (CPU, memory, disk)
import sys  # Provides system-level utilities
import time  # Useful for execution time tracking

# Adjust the Logger class to ensure logging happens dynamically
class Logger:
    """
    Custom logger class that writes both to the console and a log file.
    Ensures logs are dynamically updated and captured throughout the execution.
    """
    def __init__(self, filename):
        self.terminal = sys.stdout  # Standard console output
        try:
            self.log = open(filename, "w")  # Log file for capturing output
        except FileNotFoundError as e:
            print(f"DEBUG: Unable to create log file. Check directory structure: {e}")
            raise

    def write(self, message):
        """
        Write both to console and log file to ensure visibility of messages.
        """
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        """
        Ensure compatibility with standard file object flushing.
        """
        self.terminal.flush()
        self.log.flush()

# Note: Logging initialization will be dynamically invoked later in the program, specifically after the user inputs the working directory in Section 18.

#--------------------------------------------------------------
# End of Section 1: Import Statements
#--------------------------------------------------------------


#--------------------------------------------------------------
# Section 2: Verify No Repeated Records
#--------------------------------------------------------------

# Section 2 Overview:
# These functions help verify system resource usage and categorize files
# based on record count, ensuring efficient processing for large datasets.

def activate_logging():
    """
    Dynamically sets up logging after the user selects a working directory.
    """
    global working_directory
    try:
        log_file_path = os.path.join(working_directory, "logfile.txt")
        sys.stdout = Logger(log_file_path)
        print(f"Logging started. Output will be saved to '{log_file_path}'.\n")
        sys.stdout.flush()  # Ensure real-time updates in log file
    except Exception as e:
        print(f"DEBUG: Failed to activate logging: {e}")
        raise

def log_resource_usage():
    """
    Logs system resource usage including CPU, memory, and disk activity.
    Helps monitor system performance during execution.
    """
    print(f"DEBUG: CPU Usage: {psutil.cpu_percent()}%")
    print(f"DEBUG: Memory Usage: {psutil.virtual_memory().percent}%")
    print(f"DEBUG: Disk Usage (C:): {psutil.disk_usage('C:\\').percent}%")
    sys.stdout.flush()  # Force the log update

def determine_file_size_category(total_lines):
    """
    Categorizes files based on the number of records (lines).
    Returns 'small', 'moderate', or 'large' to optimize processing strategy.

    File Categories:
    - Small: Up to 1M records (quick processing)
    - Moderate: 1M-100M records (medium complexity)
    - Large: More than 100M records (requires advanced optimizations)
    """
    if total_lines <= 1000000:
        return 'small'
    elif total_lines <= 100000000:
        return 'moderate'
    else:
        return 'large'

#--------------------------------------------------------------
# End of Section 2: Verify No Repeated Records
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 3: Memory Optimization with Yield
#--------------------------------------------------------------

# Section 3 Overview:
# These functions process large files in smaller chunks using `yield`,
# preventing excessive memory usage compared to storing all data in lists.

def split_file_into_chunks_with_yield(file_path, chunk_size):
    """
    Efficiently processes large files by yielding small chunks.
    This prevents excessive memory usage compared to traditional list storage.

    Example:
    Input: file_path="data.txt", chunk_size=1000
    Output: Yields lists of up to 1000 lines each for processing.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            chunk = []
            for line in file:
                chunk.append(line.strip())
                if len(chunk) >= chunk_size:
                    yield chunk  # Yield the current chunk for processing
                    chunk = []  # Reset chunk after yielding
            if chunk:
                yield chunk  # Yield the remaining lines as the last chunk
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found. Ensure the file path is correct.")
    except Exception as e:
        print(f"Error: An issue occurred while chunking the file: {e}")

def process_chunks_with_yield(file_path, chunk_size, output_file):
    """
    Process chunks from a file dynamically using the `split_file_into_chunks_with_yield` function.
    Writes results to an output file incrementally, optimizing memory efficiency.
    """
    try:
        processed_lines = set()
        with open(output_file, 'w', encoding='utf-8') as output:
            for chunk in split_file_into_chunks_with_yield(file_path, chunk_size):
                for line in chunk:
                    if line not in processed_lines:
                        output.write(line + '\n')
                        processed_lines.add(line)
                print(f"DEBUG: Processed a chunk with {len(chunk)} lines.")
    except Exception as e:
        print(f"Error: Failed to process chunks due to: {e}")

#--------------------------------------------------------------
# End of Section 3: Memory Optimization with Yield
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 4: Functions to Save and Load Progress
#--------------------------------------------------------------

# Section 4 Overview:
# These functions manage saving and loading user progress using a JSON file.
# Ensures continuity when restarting the program by preserving completed steps.

def save_progress(progress, progress_file):
    """
    Save progress data to a JSON file.
    
    Parameters:
    - progress (dict): Dictionary containing the progress state.
    - progress_file (str): Path to the JSON file for storing progress.

    Writes the current state of the program's progress to the file,
    ensuring users can resume where they left off.
    """
    try:
        ensure_writable(progress_file)
        with open(progress_file, 'w', encoding='utf-8') as file:
            json.dump(progress, file, indent=4)
        print(f"DEBUG: Progress successfully saved to {progress_file}")
    except OSError as e:
        print(f"Error saving progress: {e}")
    finally:
        sys.stdout.flush()

def load_progress(progress_file):
    """
    Load existing progress data from a JSON file.
    
    Parameters:
    - progress_file (str): Path to the JSON file containing progress.

    Returns:
    - dict: The loaded progress dictionary or a fresh dictionary if the file does not exist.
    """
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
                print(f"DEBUG: Progress successfully loaded from {progress_file}")
                return data
        except json.JSONDecodeError as e:
            print(f"JSONDecodeError while loading progress: {e}")
    print(f"DEBUG: No progress file found. Starting fresh.")
    sys.stdout.flush()
    return {}

def ensure_writable(file_path):
    """
    Verify that the file path is writable before attempting to save data.

    Parameters:
    - file_path (str): Path to the file being checked.

    This function ensures that the program does not encounter permission issues when writing files.
    """
    try:
        with open(file_path, 'a'):
            pass
        print(f"DEBUG: File {file_path} is writable.")
    except OSError as e:
        print(f"Error: {file_path} is not writable. {e}")
        raise
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 4: Functions to Save and Load Progress
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 5: Initialization and Validation Functions
#--------------------------------------------------------------

# Section 5 Overview:
# Handles progress file initialization and file validation checks to ensure
# proper execution by verifying file existence and readability.

def initialize_progress_file(directory_path):
    """
    Create a new progress.json file if none exists or retrieve existing progress.

    Parameters:
    - directory_path (str): Path to the working directory where the progress file is stored.

    Returns:
    - str: Path to the progress file.
    - bool: True if the file was newly created, False if existing progress was loaded.
    """
    progress_file = os.path.join(directory_path, "progress.json")
    fresh_start = False  

    if not os.path.exists(progress_file):
        print(f"DEBUG: Progress file not found. Creating a new one at {progress_file}.")
        default_progress = {
            "processed_files": {str(i): False for i in range(1, 7)},
            "message": "Welcome! Start with Option 1: Compare two files to ensure program integrity.",
            "fresh_start": True  
        }
        try:
            with open(progress_file, 'w', encoding='utf-8') as file:
                json.dump(default_progress, file, indent=4)
            print("DEBUG: Progress file created successfully.")
            fresh_start = True  
        except OSError as e:
            print(f"Error: Unable to create progress file at '{progress_file}'. Please check directory permissions.")
            raise
    else:
        print(f"DEBUG: Progress file found at {progress_file}. Using existing progress data.")
    sys.stdout.flush()  
    return progress_file, fresh_start  

def check_file_integrity(file_path):
    """
    Verify file integrity by ensuring the file is accessible and readable.

    Parameters:
    - file_path (str): Path to the file being checked.

    This function ensures that the file is usable before processing.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for _ in file:
                pass
        print(f"DEBUG: File {file_path} is readable and intact.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' could not be found. Please ensure the path is correct.")
        raise
    except PermissionError:
        print(f"Error: Permission denied while accessing '{file_path}'. Check file permissions.")
        raise
    except Exception as e:
        print(f"Error: Unexpected issue while reading '{file_path}': {e}")
        raise
    finally:
        sys.stdout.flush()

def verify_file_integrity(file_path):
    """
    Validate file existence and check that it contains readable content.

    Parameters:
    - file_path (str): Path to the file being validated.

    Returns:
    - bool: True if the file is valid, False if it's missing or empty.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            if not any(file):  
                raise ValueError(f"The file '{file_path}' is empty. Provide a valid file with content.")
        print(f"DEBUG: File integrity verification passed for: {file_path}")
        sys.stdout.flush()
        return True
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        sys.stdout.flush()
        return False
    except ValueError as e:
        print(f"Error: {e}")
        sys.stdout.flush()
        return False
    except Exception as e:
        print(f"Error: Unexpected error: {e}.")
        sys.stdout.flush()
        return False

#--------------------------------------------------------------
# End of Section 5: Initialization and Validation Functions
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 6: Functions for Resource Monitoring
#--------------------------------------------------------------

# Section 6 Overview:
# These functions monitor system resources such as CPU, memory, and disk usage,
# helping users evaluate system performance during execution.

def log_system_resources():
    """
    Log system resource usage, including CPU, memory, and disk activity.

    This function helps assess performance efficiency and detect potential bottlenecks.
    """
    try:
        print(f"DEBUG: CPU Usage: {psutil.cpu_percent()}%")
        print(f"DEBUG: Memory Usage: {psutil.virtual_memory().percent}%")
        print(f"DEBUG: Disk Usage (C:): {psutil.disk_usage('C:\\').percent}%")
    except Exception as e:
        print(f"DEBUG: Error while logging system resources: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 6: Functions for Resource Monitoring
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 7: File Integrity Verification
#--------------------------------------------------------------

# Section 7 Overview:
# This function ensures the working directory exists and is writable.
# It checks for permissions by attempting to write and delete a temporary file.

def verify_working_directory(directory_path):
    """
    Verify that the working directory exists and is writable.

    Parameters:
    - directory_path (str): Path to the directory being checked.

    Raises an error if the directory does not exist or lacks write permissions.
    """
    try:
        if not os.path.exists(directory_path):
            raise FileNotFoundError(f"The directory '{directory_path}' does not exist.")
        test_file = os.path.join(directory_path, "test_file.tmp")
        with open(test_file, 'w') as file:
            file.write("TEST")  # Attempt to write a temporary file
        os.remove(test_file)  # Delete the temporary file after writing
        print(f"DEBUG: Working directory '{directory_path}' is verified and writable.")
    except PermissionError:
        print(f"Error: The directory '{directory_path}' does not have write permissions.")
        raise
    except Exception as e:
        print(f"Error: An issue occurred while verifying the directory: {e}")
        raise
    finally:
        sys.stdout.flush()  # Ensure logs are updated immediately

#--------------------------------------------------------------
# End of Section 7: File Integrity Verification
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 8: Progress File Management
#--------------------------------------------------------------

# Section 8 Overview:
# These functions manage the creation, updating, and loading of the progress file,
# ensuring smooth tracking of completed operations.

def create_progress_file(directory_path):
    """
    Create a progress.json file if one does not already exist.

    Parameters:
    - directory_path (str): Path where the progress file will be stored.

    Ensures a progress tracking mechanism is in place for future executions.
    """
    progress_file_path = os.path.join(directory_path, "progress.json")
    try:
        if not os.path.exists(progress_file_path):
            with open(progress_file_path, 'w', encoding='utf-8') as file:
                json.dump({"status": "initialized"}, file, indent=4)
            print(f"DEBUG: Progress file created at '{progress_file_path}'")
        else:
            print(f"DEBUG: Progress file already exists at '{progress_file_path}'")
    except Exception as e:
        print(f"DEBUG: Failed to create progress file: {e}")
    finally:
        sys.stdout.flush()

def update_progress_file(progress_file_path, status_key, status_value):
    """
    Update a specific key-value pair in the progress file.

    Parameters:
    - progress_file_path (str): Path to the progress file.
    - status_key (str): Key representing a specific progress state.
    - status_value (bool or str): The new value to assign to the key.

    Ensures the progress file maintains updated tracking throughout execution.
    """
    try:
        with open(progress_file_path, 'r+', encoding='utf-8') as file:
            progress_data = json.load(file)
            progress_data[status_key] = status_value  # Modify progress data
            file.seek(0)
            json.dump(progress_data, file, indent=4)  # Write updates
            file.truncate()  # Remove any remaining content
        print(f"DEBUG: Progress file updated: {status_key} set to {status_value}")
    except Exception as e:
        print(f"DEBUG: Failed to update progress file: {e}")
    finally:
        sys.stdout.flush()

def load_progress_file(progress_file_path):
    """
    Load and return the contents of the progress file.

    Parameters:
    - progress_file_path (str): Path to the progress file.

    Returns:
    - dict: Loaded progress data or an empty dictionary if an error occurs.
    """
    try:
        with open(progress_file_path, 'r', encoding='utf-8') as file:
            progress_data = json.load(file)
        print(f"DEBUG: Progress file loaded successfully.")
        return progress_data
    except Exception as e:
        print(f"DEBUG: Failed to load progress file: {e}")
        return {}

#--------------------------------------------------------------
# End of Section 8: Progress File Management
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 9: File Chunking Utility
#--------------------------------------------------------------

# Section 9 Overview:
# This function splits large files into smaller chunks, reducing memory usage 
# and improving processing efficiency.

def split_file_into_chunks(file_path, chunk_size, output_directory):
    """
    Split a large file into smaller, manageable chunks.

    Parameters:
    - file_path (str): Path to the file being split.
    - chunk_size (int): Number of lines per chunk.
    - output_directory (str): Directory where chunk files will be stored.

    This function ensures that massive datasets can be processed incrementally,
    reducing memory consumption and improving efficiency.
    """
    try:
        os.makedirs(output_directory, exist_ok=True)  # Ensure the output directory exists
        with open(file_path, 'r', encoding='utf-8') as file:
            chunk = []  # Temporary storage for a chunk
            chunk_index = 1  # Counter for chunk numbering
            for line in file:
                chunk.append(line)
                if len(chunk) >= chunk_size:
                    chunk_path = os.path.join(output_directory, f"chunk_{chunk_index}.txt")
                    with open(chunk_path, 'w', encoding='utf-8') as chunk_file:
                        chunk_file.writelines(chunk)
                    print(f"DEBUG: Created chunk file: {chunk_path}")
                    chunk = []  # Reset chunk after writing
                    chunk_index += 1
            if chunk:  # Write any remaining lines that didn't form a full chunk
                chunk_path = os.path.join(output_directory, f"chunk_{chunk_index}.txt")
                with open(chunk_path, 'w', encoding='utf-8') as chunk_file:
                    chunk_file.writelines(chunk)
                print(f"DEBUG: Created final chunk file: {chunk_path}")
    except Exception as e:
        print(f"DEBUG: Error during file chunking: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 9: File Chunking Utility
#--------------------------------------------------------------
#--------------------------------------------------------------
# Section 10: Progress Tracker Utility
#--------------------------------------------------------------

# Section 10 Overview:
# Provides a simple function to track and display progress in percentage.
# Helps users visualize the completion of long-running tasks.

def display_progress(current, total, task_description):
    """
    Display progress percentage for a specified task.

    Parameters:
    - current (int): The current progress count.
    - total (int): The total number of steps required.
    - task_description (str): A brief description of the task being tracked.

    Prints the completion percentage dynamically and handles zero-division errors.
    """
    try:
        if total == 0:
            raise ZeroDivisionError("Total value is zero, cannot calculate progress.")
        percentage = (current / total) * 100
        print(f"DEBUG: {task_description}: {percentage:.2f}% completed.")
    except ZeroDivisionError:
        print("DEBUG: Total value is zero, cannot calculate progress.")
    except Exception as e:
        print(f"DEBUG: Error while displaying progress: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 10: Progress Tracker Utility
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 11: Duplicate Extraction
#--------------------------------------------------------------

# Section 11 Overview:
# Identifies duplicate records in an input file and extracts them into a separate file.
# Useful for dataset cleanup and duplicate verification.

def extract_duplicates(input_file, output_duplicates_file):
    """
    Identify and extract duplicate records from an input file.

    Parameters:
    - input_file (str): Path to the file containing records.
    - output_duplicates_file (str): Path to the file where duplicate entries will be saved.

    Reads the file, tracks seen records, and extracts repeated entries.
    """
    try:
        seen_records = set()  # Stores unique records
        duplicates = []  # List to store duplicate entries

        with open(input_file, 'r', encoding='utf-8') as file:
            for line in file:
                record = line.strip()
                if record in seen_records:
                    duplicates.append(record)  # Add duplicate to list
                else:
                    seen_records.add(record)  # Store new record

        with open(output_duplicates_file, 'w', encoding='utf-8') as duplicates_file:
            duplicates_file.writelines(record + '\n' for record in duplicates)

        print(f"DEBUG: Found {len(duplicates)} duplicates. Results saved to '{output_duplicates_file}'")
    except Exception as e:
        print(f"DEBUG: Error during duplicate extraction: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 11: Duplicate Extraction
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 12: Spot Checks
#--------------------------------------------------------------

# Section 12 Overview:
# Allows users to perform random sampling of file records to verify data accuracy.
# Helps check for anomalies before proceeding with further processing.

import random  # Required for selecting random samples

def perform_spot_checks(input_file, number_of_checks):
    """
    Perform random spot checks on an input file to validate its contents.

    Parameters:
    - input_file (str): Path to the file containing records.
    - number_of_checks (int): Number of random entries to sample.

    If fewer records exist than requested checks, adjusts accordingly.
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Adjust check count if file has fewer records than requested
        if len(lines) < number_of_checks:
            print(f"DEBUG: Not enough records for {number_of_checks} checks, performing {len(lines)} checks instead.")
            number_of_checks = len(lines)

        random_samples = random.sample(lines, number_of_checks)

        print(f"DEBUG: Spot Checks Results:")
        for index, sample in enumerate(random_samples, start=1):
            print(f"Sample {index}: {sample.strip()}")
    except Exception as e:
        print(f"DEBUG: Error during spot checks: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 12: Spot Checks
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 13: String Search Utility
#--------------------------------------------------------------

def search_string_in_file(file_path, search_string, output_file):
    """
    Search for a specific string in a file and save matching lines to an output file.
    """
    try:
        matches = []
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                if search_string in line:
                    matches.append(line.strip())
        with open(output_file, 'w', encoding='utf-8') as output:
            output.writelines(match + '\n' for match in matches)
        print(f"DEBUG: Found {len(matches)} matches for '{search_string}'. Results saved to '{output_file}'")
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found. Ensure the path is correct.")
        raise
    except Exception as e:
        print(f"DEBUG: Error while searching for string: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 13: String Search Utility
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 14: File Rename and Move Utility
#--------------------------------------------------------------

def rename_and_move_file(source_file_path, destination_directory, new_file_name):
    """
    Rename and move a file to a new directory with a specified name.
    """
    try:
        os.makedirs(destination_directory, exist_ok=True)
        new_file_path = os.path.join(destination_directory, new_file_name)
        shutil.move(source_file_path, new_file_path)
        print(f"DEBUG: File successfully renamed and moved to: {new_file_path}")
    except FileNotFoundError:
        print(f"Error: Source file '{source_file_path}' not found. Ensure the path is correct.")
        raise
    except Exception as e:
        print(f"DEBUG: Error during file rename and move: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 14: File Rename and Move Utility
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 15: Memory Logging and Diagnostics
#--------------------------------------------------------------
import psutil
import gc

def log_memory_usage(stage_name):
    """
    Logs current memory usage and garbage collection statistics.

    Purpose:
    - Monitors system memory consumption at key execution stages.
    - Helps diagnose performance bottlenecks in large dataset processing.
    - Tracks garbage collection behavior for optimization.

    Parameters:
    - stage_name (str): Name of the program stage where logging occurs.

    Output:
    - Displays used and available memory in MB.
    - Reports garbage collection activity.
    """
    try:
        memory = psutil.virtual_memory()
        print(f"DEBUG: [{stage_name}] Memory Usage - Used: {memory.used // 1024**2} MB, Available: {memory.available // 1024**2} MB")
        print(f"DEBUG: [{stage_name}] Total Memory: {memory.total // 1024**2} MB, Percentage Used: {memory.percent}%")
        print(f"DEBUG: [{stage_name}] Garbage Collection - Thresholds: {gc.get_threshold()}, Counts: {gc.get_count()}")
        sys.stdout.flush()  # Ensure logs are reflected in real-time
    except Exception as e:
        print(f"DEBUG: Error logging memory usage: {e}")
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 15: Memory Logging and Diagnostics
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 16: Batch Processing Mode
#--------------------------------------------------------------

def batch_processing_mode(working_directory, input_file_name, output_directory):
    """
    Automates sequential execution of key deduplication tasks.

    Purpose:
    - Executes multiple operations in batch (cleaning, duplicates extraction, spot checks, renaming).
    - Reduces manual interaction during processing.
    - Ensures efficient handling of large datasets.

    Parameters:
    - working_directory (str): Base directory containing the input dataset.
    - input_file_name (str): Name of the raw dataset file to be processed.
    - output_directory (str): Destination folder for cleaned files.

    Steps:
    1. Verify and clean records.
    2. Extract duplicates.
    3. Perform spot checks for random data validation.
    4. Rename and move cleaned files.

    Output:
    - Saves cleaned and deduplicated files in designated directories.
    - Provides debug logs for analysis.
    """
    try:
        print("DEBUG: Starting Batch Processing Mode...")

        # Step 1: Verify and clean records
        clean_file_path = os.path.join(output_directory, "cleaned_file.txt")
        process_chunks_with_yield(
            file_path=os.path.join(working_directory, input_file_name),
            chunk_size=1000,
            output_file=clean_file_path,
        )
        print("DEBUG: Records verified and cleaned.")

        # Step 2: Extract duplicates
        duplicates_file_path = os.path.join(output_directory, "duplicates.txt")
        extract_duplicates(clean_file_path, duplicates_file_path)
        print("DEBUG: Duplicate extraction complete.")

        # Step 3: Perform spot checks
        perform_spot_checks(clean_file_path, 10)
        print("DEBUG: Spot checks completed.")

        # Step 4: Rename and move cleaned file
        renamed_file_path = os.path.join(output_directory, "final_cleaned_file.txt")
        rename_and_move_file(clean_file_path, output_directory, "final_cleaned_file.txt")
        print(f"DEBUG: Cleaned file renamed and moved to '{renamed_file_path}'.")

    except Exception as e:
        print(f"DEBUG: Error during batch processing: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 16: Batch Processing Mode
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 17: Logging Setup
#--------------------------------------------------------------

def initialize_logging(working_directory):
    """
    Initializes logging system to track execution flow.

    Purpose:
    - Redirects standard output to a log file.
    - Preserves debugging information for troubleshooting.
    - Ensures all program events are documented for future analysis.

    Parameters:
    - working_directory (str): Directory where logs should be stored.

    Output:
    - Creates 'logfile.txt' in the working directory.
    - Captures runtime messages, errors, and debugging outputs.
    """
    try:
        log_file_path = os.path.join(working_directory, "logfile.txt")
        sys.stdout = Logger(log_file_path)
        print(f"Logging started. Output will be saved to '{log_file_path}'.\n")
        sys.stdout.flush()
    except Exception as e:
        print(f"DEBUG: Failed to initialize logging: {e}")
        raise

#--------------------------------------------------------------
# End of Section 17: Logging Setup
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 18: Main Menu Logic
#--------------------------------------------------------------

#--------------------------------------------------------------
# Overview:
# This section handles the main menu logic, user interactions, 
# and execution flow for various deduplication tasks.
#
# Key Features:
# - Manages input handling for dataset verification, duplicate extraction, and validation.
# - Integrates memory tracking (Section 15) to monitor system resource consumption.
# - Ensures structured execution with clear debug logging.
# - Supports dataset size constraints, optimized around benchmark findings (100M records).
#
# For modifications:
# - If updating execution flow, review memory diagnostics for efficiency.
# - If altering functions, ensure consistency with Sections 15–17.
#
#--------------------------------------------------------------

# Global variable to store the working directory
working_directory = None

def normalize_string(string):
    """
    Normalize strings by removing leading/trailing spaces and collapsing multiple spaces.
    """
    return " ".join(string.strip().split())

def get_working_directory():
    """
    Manage the working directory: use the previously entered directory or prompt for a new one.
    """
    global working_directory
    if not working_directory:  
        working_directory = input("Enter the path for the working directory: ").strip()
        if not os.path.exists(working_directory):
            os.makedirs(working_directory, exist_ok=True)
        print(f"DEBUG: Working directory set to: {working_directory}")
    else:
        use_previous = input(f"Use the previously entered working directory ({working_directory})? (yes/no): ").strip().lower()
        if use_previous != "yes":
            working_directory = input("Enter the path for the working directory: ").strip()
            if not os.path.exists(working_directory):
                os.makedirs(working_directory, exist_ok=True)
            print(f"DEBUG: Working directory updated to: {working_directory}")
    sys.stdout.flush()
    return working_directory

def activate_logging():
    """
    Set up logging dynamically using the global `working_directory`.
    Clears the existing log file content at the start of each session.
    """
    global working_directory
    try:
        log_file_path = os.path.join(working_directory, "logfile.txt")
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            pass  
        sys.stdout = Logger(log_file_path)
        print(f"Logging started. Output will be saved to '{log_file_path}'.\n")
        sys.stdout.flush()
    except Exception as e:
        print(f"DEBUG: Failed to activate logging: {e}")
        raise

# ✅ Log memory usage at startup
log_memory_usage("Startup")
def initialize_progress_file(directory):
    """
    Initialize the progress.json file with an option to start fresh or continue.
    """
    progress_file = os.path.join(directory, "progress.json")

    if os.path.isfile(progress_file):
        reset_choice = input("Existing progress found. Do you want to continue (c) or start fresh (f)? ").strip().lower()
        if reset_choice == "f":
            os.remove(progress_file)
            print("DEBUG: Progress file reset. Starting fresh.")

    if not os.path.isfile(progress_file):
        progress_data = { "processed_files": {str(i): False for i in range(1, 7)} }
        with open(progress_file, 'w', encoding='utf-8') as file:
            json.dump(progress_data, file, indent=4)
        print(f"DEBUG: Progress file created at '{progress_file}'.")
    
    sys.stdout.flush()
    return progress_file
def load_progress(progress_file):
    """
    Load progress data from the progress.json file.
    """
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r', encoding='utf-8') as file:
                return json.load(file)
        print(f"DEBUG: Progress file '{progress_file}' not found. Starting fresh progress.")
        return { "processed_files": {str(i): False for i in range(1, 7)} }
    except Exception as e:
        print(f"DEBUG: Error loading progress file: {e}. Starting fresh progress.")
        return { "processed_files": {str(i): False for i in range(1, 7)} }

def save_progress(progress_data, progress_file):
    """
    Save progress data to the progress.json file.
    """
    try:
        with open(progress_file, 'w', encoding='utf-8') as file:
            json.dump(progress_data, file, indent=4)
        print("DEBUG: Progress file updated successfully.")
        sys.stdout.flush()
    except Exception as e:
        print(f"DEBUG: Error saving progress file: {e}")
        sys.stdout.flush()

# ✅ Log memory usage after progress file initialization
log_memory_usage("Progress Initialization")
def main():
    """
    Main function for the Deduplication Program.
    """
    print("=" * 60)
    print("Welcome to FusionLogic Deduplicator Search Version!")
    print("Advanced Deduplication Tool")
    print("Efficiently clean, validate, and verify datasets.")
    print("=" * 60)
    sys.stdout.flush()

    global working_directory
    working_directory = get_working_directory()

    activate_logging()
    progress_file = initialize_progress_file(working_directory)  # Corrected single return value

    progress_data = load_progress(progress_file)

    while True:
        print("\nMain Menu:")
        print("1. Verify and Clean Records")
        print("2. Extract and Verify Duplicates")
        print("3. Perform Spot Checks to Verify Duplicates")
        print("4. Search for a Specific String in the File")
        print("5. Rename and Move Cleaned File")
        print("6. Clear the Working Directory")
        print("7. Exit")
        sys.stdout.flush()

        option = input("Enter the number of your choice: ").strip()
        if option == '1':
            verify_and_clean_records(progress_data, progress_file)
            log_memory_usage("Post Deduplication")

        elif option == '2':
            extract_duplicates(progress_data, progress_file)
            log_memory_usage("Post Duplicate Extraction")

        elif option == '3':
            perform_spot_checks(progress_data, progress_file)
            log_memory_usage("Post Spot Checks")

        elif option == '4':
            if not progress_data["processed_files"]["2"]:
                user_choice = input("WARNING: You are searching before verifying duplicates. Recommended to run Option 2 first. Proceed anyway? (yes/no): ").strip().lower()
                if user_choice != "yes":
                    print("DEBUG: Redirecting to Option 2...")
                    extract_duplicates(progress_data, progress_file)
            search_string_in_file()
            log_memory_usage("Post Search Execution")

        elif option == '5':
            rename_and_move_file(progress_data, progress_file)
            log_memory_usage("Post Rename & Move")

        elif option == '6':
            clear_working_directory(progress_data, progress_file)
            log_memory_usage("Post Clear Directory")

        elif option == '7':
            log_memory_usage("Pre Main Menu Exit")
            print("DEBUG: Exiting the program. Goodbye!")
            sys.stdout.flush()
            break

        else:
            print("DEBUG: Invalid choice. Please select a valid menu option.")
            sys.stdout.flush()
def verify_and_clean_records(progress_data, progress_file):
    """
    Verify and clean records by checking for duplicates and creating a cleaned file.
    Includes normalization logic for whitespace variations and user feedback during processing.
    """
    print("DEBUG: Starting the process to verify and clean records.")
    sys.stdout.flush()

    directory_path = input("Enter the path to the directory containing the file for deduplication: ").strip()
    file_name = input("Enter the name of the file for deduplication (e.g., 'input.txt'): ").strip()
    input_file_path = os.path.join(directory_path, file_name)
    cleaned_output_path = os.path.join(working_directory, "deduplicatorlogic.txt")

    if not os.path.isfile(input_file_path):
        print(f"DEBUG: Error: File '{input_file_path}' not found. Please provide a valid file.")
        sys.stdout.flush()
        return

    try:
        unique_records = set()
        with open(input_file_path, 'r', encoding='utf-8') as input_file:
            lines = input_file.readlines()
            print(f"DEBUG: File integrity check passed. Total lines in file: {len(lines)}")

            with tqdm(total=len(lines), desc="Processing lines", unit="line") as pbar, \
                    open(cleaned_output_path, 'w', encoding='utf-8') as cleaned_file:
                for line in lines:
                    normalized_line = normalize_string(line.strip()) 
                    if normalized_line not in unique_records:
                        cleaned_file.write(line)
                        unique_records.add(normalized_line)
                    pbar.update(1)  

        print(f"DEBUG: Cleaned file '{cleaned_output_path}' successfully created.")
        print(f"DEBUG: Total lines processed: {len(lines)}")
        print(f"DEBUG: Unique lines saved: {len(unique_records)}")
        sys.stdout.flush()

        # ✅ Log memory usage after deduplication process
        log_memory_usage("Post Deduplication")

        progress_data["processed_files"]["1"] = True
        save_progress(progress_data, progress_file)
    except Exception as e:
        print(f"DEBUG: Error during record verification and cleaning: {e}")
        sys.stdout.flush()
def extract_duplicates(progress_data, progress_file):
    """
    Extract and verify duplicate records, with the ability to rerun cleaning if duplicates are detected.
    """
    print("DEBUG: Starting extraction of duplicates...")
    sys.stdout.flush()

    cleaned_file_path = os.path.join(working_directory, "deduplicatorlogic.txt")
    if not os.path.isfile(cleaned_file_path):
        print(f"DEBUG: Error: Cleaned file '{cleaned_file_path}' not found. Please run Option 1 first.")
        sys.stdout.flush()
        return

    try:
        duplicates_found = []
        unique_records = set()

        with open(cleaned_file_path, 'r', encoding='utf-8') as cleaned_file:
            for line in cleaned_file:
                stripped_line = line.strip()
                if stripped_line in unique_records:
                    duplicates_found.append(stripped_line)
                else:
                    unique_records.add(stripped_line)

        results_output_file = os.path.join(working_directory, "extracted_duplicates.txt")
        with open(results_output_file, 'w', encoding='utf-8') as results_file:
            if duplicates_found:
                for duplicate in duplicates_found:
                    results_file.write(duplicate + '\n')
                print(f"DEBUG: Duplicate extraction complete. Results saved to '{results_output_file}'.")
                print(f"DEBUG: {len(duplicates_found)} duplicates found.")
            else:
                results_file.write("No duplicates found.\n")
                print("DEBUG: No duplicates found.")

        sys.stdout.flush()

        # ✅ Log memory usage after duplicate extraction process
        log_memory_usage("Post Duplicate Extraction")

        progress_data["processed_files"]["2"] = True
        save_progress(progress_data, progress_file)

    except Exception as e:
        print(f"DEBUG: Error during duplicate extraction process: {e}")
        sys.stdout.flush()
def perform_spot_checks(progress_data, progress_file):
    """
    Perform spot checks on random samples from the cleaned file.
    """
    print("DEBUG: Starting spot checks...")
    sys.stdout.flush()

    cleaned_file_path = os.path.join(working_directory, "deduplicatorlogic.txt")
    if not os.path.isfile(cleaned_file_path):
        print(f"DEBUG: Error: Cleaned file '{cleaned_file_path}' not found. Please run Option 1 first.")
        sys.stdout.flush()
        return

    try:
        import random
        num_checks = int(input("Enter the number of random checks to perform: ").strip())
        with open(cleaned_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        sampled_lines = random.sample(lines, min(num_checks, len(lines)))  
        print("\nDEBUG: Spot Check Results:")
        for i, line in enumerate(sampled_lines, start=1):
            print(f"Sample {i}: {line.strip()}")

        print("DEBUG: Spot checks completed successfully!")
        sys.stdout.flush()

        # ✅ Log memory usage after performing spot checks
        log_memory_usage("Post Spot Checks")

        progress_data["processed_files"]["3"] = True
        save_progress(progress_data, progress_file)
    except Exception as e:
        print(f"DEBUG: Error during spot checks: {e}")
        sys.stdout.flush()
def search_string_in_file():
    """
    Search for a specific string or pattern in the cleaned file.
    """
    print("DEBUG: Starting string search...")
    sys.stdout.flush()

    file_path = os.path.join(working_directory, "deduplicatorlogic.txt")
    if not os.path.isfile(file_path):
        print(f"DEBUG: Error: File '{file_path}' does not exist.")
        sys.stdout.flush()
        return

    try:
        search_string = input("Enter the string or pattern to search for: ").strip()
        matches = []

        with open(file_path, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, start=1):
                if search_string in normalize_string(line.strip()):  
                    matches.append((line_number, line.strip()))

        results_output_file = os.path.join(working_directory, "search_results.txt")
        with open(results_output_file, 'w', encoding='utf-8') as results_file:
            if matches:
                for match in matches:
                    results_file.write(f"Line {match[0]}: {match[1]}\n")
                print(f"DEBUG: Search results saved to '{results_output_file}'.")
            else:
                results_file.write("No matches found.\n")
                print("DEBUG: No matches found.")      
        sys.stdout.flush()

        # ✅ Log memory usage after search function completes
        log_memory_usage("Post Search Function")
    except Exception as e:
        print(f"DEBUG: Error during string search: {e}")
        sys.stdout.flush()
def rename_and_move_file(progress_data, progress_file):
    """
    Rename and optionally move the cleaned file to a user-specified directory, while updating progress.
    """
    print("DEBUG: Starting rename and move operation...")
    sys.stdout.flush()

    default_file_name = "deduplicatorlogic-s.txt"
    cleaned_file_path = os.path.join(working_directory, "deduplicatorlogic.txt")

    if not os.path.isfile(cleaned_file_path):
        print(f"DEBUG: Error: File '{cleaned_file_path}' not found. Please run Option 1 first.")
        sys.stdout.flush()
        return

    try:
        new_directory = input("Enter the path to the directory where you want to move the file: ").strip()
        new_file_name = input(f"Enter the new name for the cleaned file (default: {default_file_name}): ").strip()

        if not new_file_name:
            new_file_name = default_file_name

        new_file_path = os.path.join(new_directory, new_file_name)
        counter = 1
        while os.path.exists(new_file_path):
            name, ext = os.path.splitext(new_file_name)
            new_file_path = os.path.join(new_directory, f"{name}({counter}){ext}")
            counter += 1

        if not os.path.exists(new_directory):
            os.makedirs(new_directory, exist_ok=True)

        shutil.move(cleaned_file_path, new_file_path)
        print(f"DEBUG: File successfully moved and renamed to: {new_file_path}")
        sys.stdout.flush()

        # ✅ Log memory usage after renaming/moving process
        log_memory_usage("Post Rename & Move")

        progress_data["processed_files"]["5"] = True
        save_progress(progress_data, progress_file)
    except Exception as e:
        print(f"DEBUG: Error during renaming/moving process: {e}")
        sys.stdout.flush()
def clear_working_directory(progress_data, progress_file):
    """
    Fully reset the working directory by deleting all files except for logfile.txt.
    """
    print("DEBUG: Clearing the working directory...")
    sys.stdout.flush()

    deduplicator_file_path = os.path.join(working_directory, "deduplicatorlogic.txt")
    if os.path.exists(deduplicator_file_path):
        confirmation = input("This action will permanently delete the deduplicator file. Continue? (yes/no): ").strip().lower()
        if confirmation != "yes":
            print("DEBUG: Clearing operation aborted by the user.")
            sys.stdout.flush()
            return

    confirmation = input("Are you sure you want to clear the working directory? This action cannot be undone. (yes/no): ").strip().lower()
    if confirmation != "yes":
        print("DEBUG: Clearing operation aborted by the user.")
        sys.stdout.flush()
        return

    try:
        files = [f for f in os.listdir(working_directory) if f != "logfile.txt"]
        for file in files:
            file_path = os.path.join(working_directory, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

        print("DEBUG: Working directory cleared successfully, except for 'logfile.txt'.")
        sys.stdout.flush()

        # ✅ Log memory usage after clearing the working directory
        log_memory_usage("Post Clear Directory")

        progress_data["processed_files"]["6"] = True
        save_progress(progress_data, progress_file)
    except Exception as e:
        print(f"DEBUG: Error during clearing process: {e}")
        sys.stdout.flush()
if __name__ == "__main__":
    main()
#--------------------------------------------------------------
# End of Section 18: Main Menu Logic
#--------------------------------------------------------------

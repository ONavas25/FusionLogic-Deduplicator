#--------------------------------------------------------------
# Section 1: Import Statements
#--------------------------------------------------------------

import os
import json
from itertools import islice
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import random
import gc
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import aiofiles
import asyncio
import mmap
import shutil
import psutil  # For memory and CPU diagnostics
import sys  # For logging setup
import time  # For tracking timestamps

# Adjust the Logger class to ensure logging happens dynamically
class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout  # Standard console output
        try:
            self.log = open(filename, "w")  # Log file for capturing output
        except FileNotFoundError as e:
            print(f"DEBUG: Unable to create log file. Check directory structure: {e}")
            raise

    def write(self, message):
        # Write both to console and log file
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        # Ensure compatibility with standard file object
        self.terminal.flush()
        self.log.flush()

# Note: Logging initialization will be dynamically invoked later in the program, specifically after the user inputs the working directory in Section 17.

#--------------------------------------------------------------
# End of Section 1: Import Statements
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 2: Verify No Repeated Records
#--------------------------------------------------------------

import gc  # Import garbage collection module
import psutil  # Import psutil for resource tracking
import sys  # Import system module for logging setup
import time  # Import time module for tracking timestamps
from tqdm import tqdm  # Import tqdm for progress bar

def activate_logging():
    """
    Set up logging dynamically using the global `working_directory`.
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
    Log system resource usage including CPU, memory, and disk activity.
    """
    print(f"DEBUG: CPU Usage: {psutil.cpu_percent()}%")
    print(f"DEBUG: Memory Usage: {psutil.virtual_memory().percent}%")
    print(f"DEBUG: Disk Usage (C:): {psutil.disk_usage('C:\\').percent}%")
    sys.stdout.flush()  # Force the log update

def determine_file_size_category(total_lines):
    """
    Determine the file size category based on the number of records (lines).
    Returns 'small', 'moderate', or 'large'.
    """
    if total_lines <= 1000000:  # Files with up to 1M records
        return 'small'
    elif total_lines <= 100000000:  # Files with 10–100M records
        return 'moderate'
    else:  # Files larger than 100M records
        return 'large'

async def check_union_file_with_chunks(directory_path, default_chunk_size=100000):
    """
    Verify no repeated records exist in the union file by processing it in chunks.
    Implements a sequential approach for stability and includes saving chunk files.
    Enhanced with memory logging and frequent diagnostics for better monitoring.
    """
    union_file_path = os.path.join(directory_path, "uniout.txt")
    chunk_dir = os.path.join(directory_path, "chunks")
    veriout_file_path = os.path.join(directory_path, "veriout.txt")  # Path for the verification output file

    # Clean the chunks directory before processing
    if os.path.exists(chunk_dir):
        for filename in os.listdir(chunk_dir):
            file_path = os.path.join(chunk_dir, filename)
            if os.path.isfile(file_path):
                os.unlink(file_path)  # Delete old chunk files
        print(f"Cleaned up the existing chunk directory: {chunk_dir}")
    else:
        os.makedirs(chunk_dir, exist_ok=True)  # Create the directory if it doesn't exist

    print(f"DEBUG: Verification output file path: {veriout_file_path}")
    sys.stdout.flush()  # Ensure real-time log updates

    # Count total lines to determine file size category
    total_lines = sum(1 for _ in open(union_file_path, 'r', encoding='utf-8'))
    size_category = determine_file_size_category(total_lines)
    print(f"DEBUG: Total lines: {total_lines}. File size category: {size_category}")
    sys.stdout.flush()

    # Adjust chunk size based on file size category
    if size_category == 'small':
        chunk_size = 500000  # Larger chunks for smaller datasets
    elif size_category == 'moderate':
        chunk_size = 1000000  # Even larger chunks for moderate datasets
    else:
        chunk_size = default_chunk_size

    def split_file_into_chunks():
        """
        Split the union file into larger chunks for processing with progress updates.
        Save each chunk as a physical file in the chunks directory.
        """
        total_chunks = (total_lines + chunk_size - 1) // chunk_size  # Ceiling division to calculate chunks
        print(f"Estimated total chunks: {total_chunks}")
        sys.stdout.flush()

        with open(union_file_path, 'r', encoding='utf-8') as file:
            chunk = []
            chunk_count = 0  # Track the number of chunks created
            for line_index, line in tqdm(enumerate(file, start=1), total=total_lines, file=sys.stdout):
                chunk.append(line.strip())
                if len(chunk) >= chunk_size:
                    # Save the chunk to a file in the chunks directory
                    chunk_file_path = os.path.join(chunk_dir, f"chunk_{chunk_count + 1}.txt")
                    with open(chunk_file_path, 'w', encoding='utf-8') as chunk_file:
                        chunk_file.writelines(line + '\n' for line in chunk)
                    yield chunk  # Yield the chunk for further processing
                    chunk = []  # Reset chunk
                    chunk_count += 1
                    print(f"Checkpoint: Created {chunk_count} of {total_chunks} chunks.")
                    sys.stdout.flush()
                # Log resource usage every 10,000 lines processed
                if line_index % 10000 == 0:
                    log_resource_usage()

            if chunk:
                # Save any remaining lines in a final chunk file
                chunk_file_path = os.path.join(chunk_dir, f"chunk_final.txt")
                with open(chunk_file_path, 'w', encoding='utf-8') as chunk_file:
                    chunk_file.writelines(line + '\n' for line in chunk)
                yield chunk  # Yield the final chunk
                chunk_count += 1
            if chunk_count == 0:
                print("DEBUG: No chunks were created! Ensure the input file is not empty.")
            print("Chunk creation completed and saved to disk!")
            sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 2: Verify No Repeated Records
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 3: Functions to Save and Load Progress
#--------------------------------------------------------------

def save_progress(progress, progress_file):
    """
    Save progress to a JSON file.
    """
    try:
        ensure_writable(progress_file)
        with open(progress_file, 'w', encoding='utf-8') as file:
            json.dump(progress, file)
        print(f"DEBUG: Progress successfully saved to {progress_file}")
    except OSError as e:
        print(f"Error saving progress: {e}")
    finally:
        sys.stdout.flush()

def load_progress(progress_file):
    """
    Load progress from a JSON file.
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
    Ensure the file path is writable.
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
# End of Section 3: Functions to Save and Load Progress
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 3.1: Functions for Initialization and Validation
#--------------------------------------------------------------

def initialize_progress_file(directory_path):
    """
    Create a progress.json file if it does not already exist and mark it as a fresh start.
    """
    progress_file = os.path.join(directory_path, "progress.json")
    fresh_start = False  # Flag to track if the file was freshly created

    if not os.path.exists(progress_file):
        print(f"DEBUG: Progress file not found. Creating a new one at {progress_file}.")
        default_progress = {
            "processed_files": {
                "file1": False,
                "file2": False,
                "file3": False,
                "file4": False,
                "file5": False,
                "file6": False
            },
            "message": "You are running the program for the first time. Please start with Option 1: Compare two files to ensure program integrity.",
            "fresh_start": True  # Indicate this is a fresh start
        }
        try:
            with open(progress_file, 'w', encoding='utf-8') as file:
                json.dump(default_progress, file, indent=4)
            print("Progress file created successfully.")
            fresh_start = True  # Mark the fresh start explicitly
        except OSError as e:
            print(f"Error creating progress file: {e}")
    else:
        print(f"DEBUG: Progress file found at {progress_file}. Using existing progress data.")
    sys.stdout.flush()  # Ensure updates are reflected in the log file
    return progress_file, fresh_start  # Ensure it returns both values

def check_file_integrity(file_path):
    """
    Check the integrity of the input file to ensure it is readable and intact.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                pass
        print(f"DEBUG: File {file_path} is readable and intact.")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        raise
    finally:
        sys.stdout.flush()  # Ensure logs are updated immediately

def verify_file_integrity(file_path):
    """
    Verify the integrity of the file by checking existence and readability.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            if not any(file):  # Check if file is empty
                raise ValueError("File is empty")
        print(f"DEBUG: File integrity verification passed for: {file_path}")
        sys.stdout.flush()
        return True
    except FileNotFoundError as e:
        print(f"DEBUG: {e}")
        sys.stdout.flush()
        return False
    except Exception as e:
        print(f"DEBUG: Error during file integrity verification: {e}")
        sys.stdout.flush()
        return False
#--------------------------------------------------------------
# End of Section 3.1: Functions for Initialization and Validation
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 4: Compare Two Files (Revised with Resource Logging)
#--------------------------------------------------------------

from tqdm import tqdm

async def compare_files(file1_path, file2_path, union_file_path, match_output_path, progress_file, batch_size=10000, update_interval=50000):
    """
    Compare two files to generate a union file of unique lines and a match file of common lines.
    Includes enhanced resource monitoring for memory and CPU utilization.
    """
    unique_lines = set()
    match_lines = set()

    async def process_file(file_path, file_desc, total_lines, unique_lines, match_lines, union_file, match_file, batch_size, update_interval):
        """
        Process a single file to find unique and matching lines.
        """
        async with aiofiles.open(file_path, mode='r', encoding='utf-8') as file:
            batch = []
            match_batch = []
            line_number = 0  # To manually track line numbers

            # Log initial system resource usage
            print(f"DEBUG: Starting {file_desc}. Initial resource utilization:")
            log_resource_usage()

            # Initialize tqdm for progress tracking
            with tqdm(total=total_lines, desc=file_desc, ncols=100, mininterval=0.5, file=sys.stdout) as progress_bar:
                while True:
                    line = await file.readline()
                    if not line:
                        break  # End of file
                    line_number += 1

                    stripped_line = line.strip()
                    if stripped_line in unique_lines:
                        match_batch.append(stripped_line)
                        match_lines.add(stripped_line)
                    else:
                        batch.append(stripped_line)
                        unique_lines.add(stripped_line)

                    # Write batches to files when they reach the batch size
                    if len(batch) >= batch_size:
                        await union_file.write('\n'.join(l for l in batch if l) + '\n')
                        batch.clear()
                        gc.collect()
                    if len(match_batch) >= batch_size:
                        await match_file.write('\n'.join(l for l in match_batch if l) + '\n')
                        match_batch.clear()
                        gc.collect()

                    # Log resource usage and progress every update_interval lines
                    if line_number % update_interval == 0:
                        print(f"DEBUG: Processed {line_number} lines so far for {file_desc}.")
                        log_resource_usage()

                    # Update tqdm progress bar
                    progress_bar.update(1)

                # Write any remaining lines in the batches
                if batch:
                    await union_file.write('\n'.join(l for l in batch if l) + '\n')
                if match_batch:
                    await match_file.write('\n'.join(l for l in match_batch if l) + '\n')

                # Log final resource utilization after processing the file
                print(f"DEBUG: Finished processing {file_desc}. Total lines processed: {line_number}. Final resource utilization:")
                log_resource_usage()
                sys.stdout.flush()

    try:
        # Count total lines in both files for progress tracking
        total_lines_file1 = sum(1 for _ in open(file1_path, 'r', encoding='utf-8'))
        total_lines_file2 = sum(1 for _ in open(file2_path, 'r', encoding='utf-8'))

        print(f"DEBUG: Starting comparison process...")
        log_resource_usage()  # Baseline resource usage
        print(f"DEBUG: File 1 ({file1_path}) has {total_lines_file1} lines.")
        print(f"DEBUG: File 2 ({file2_path}) has {total_lines_file2} lines.")
        sys.stdout.flush()

        # Open output files and process each input file
        async with aiofiles.open(union_file_path, mode='w', encoding='utf-8') as union_file, \
                   aiofiles.open(match_output_path, mode='w', encoding='utf-8') as match_file:
            # Process the first file
            await process_file(file1_path, "Processing File 1", total_lines_file1, unique_lines, match_lines, union_file, match_file, batch_size, update_interval)
            # Process the second file
            await process_file(file2_path, "Processing File 2", total_lines_file2, unique_lines, match_lines, union_file, match_file, batch_size, update_interval)

    except OSError as e:
        print(f"Error reading the files {file1_path} or {file2_path}: {e}")
        sys.stdout.flush()

    # Final resource diagnostic summary
    print("DEBUG: File comparison and union creation completed successfully! Final resource utilization:")
    log_resource_usage()
    sys.stdout.flush()
#--------------------------------------------------------------
# End of Section 4: Compare Two Files (Revised with Resource Logging)
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 5: Write Matches and Unique Records
#--------------------------------------------------------------
def write_matches_as_you_go(output_path, words):
    """
    Write matches incrementally to the output file.
    """
    try:
        ensure_writable(output_path)
        with open(output_path, 'a', encoding='utf-8') as output_file:
            os.set_inheritable(output_file.fileno(), False)
            output_file.write('\n'.join(words) + '\n')
        print(f"DEBUG: Matches written to {output_path}")
    except OSError as e:
        print(f"Error writing to file {output_path}: {e}")
    finally:
        sys.stdout.flush()  # Ensure logs are updated in real-time

def write_union_as_you_go(output_path, words):
    """
    Write unique records incrementally to the output file.
    """
    try:
        ensure_writable(output_path)
        with open(output_path, 'a', encoding='utf-8') as output_file:
            os.set_inheritable(output_file.fileno(), False)
            output_file.write('\n'.join(words) + '\n')
        print(f"DEBUG: Unique records written to {output_path}")
    except OSError as e:
        print(f"Error writing to file {output_path}: {e}")
    finally:
        sys.stdout.flush()  # Ensure logs are updated in real-time

#--------------------------------------------------------------
# End of Section 5: Write Matches and Unique Records
#--------------------------------------------------------------

# --------------------------------------------------------------
# Section 6: Process File Chunks
# --------------------------------------------------------------
def normalize_line(line):
    """
    Normalize the line by stripping leading and trailing whitespace.
    """
    return line.strip()

def process_chunk(chunk_file, other_file_set, match_output_path, unique_output_path, unique_set):
    """
    Process a chunk of lines, identifying matches and unique lines, then write results.
    Includes memory logging and garbage collection diagnostics.
    Enhanced with more frequent progress logging and diagnostic insights.
    """
    matches = []
    unique = []
    for line in chunk_file:
        normalized_line = normalize_line(line)
        if normalized_line in other_file_set:
            matches.append(line)
        elif normalized_line not in unique_set:
            unique.append(line)
            unique_set.add(normalized_line)
    
    # Log memory usage and garbage collection stats periodically
    log_memory_usage()  # Log memory usage
    log_garbage_collection()  # Log garbage collection stats
    
    # Log progress checkpoint after processing chunk
    print(f"DEBUG: Processed {len(chunk_file)} lines from the current chunk.")
    sys.stdout.flush()  # Ensure logs are reflected in real-time
    
    # Write results for matches and unique lines
    write_matches_as_you_go(match_output_path, matches)
    write_union_as_you_go(unique_output_path, unique)
    return matches, unique

# --------------------------------------------------------------
# End of Section 6: Process File Chunks
# --------------------------------------------------------------

#--------------------------------------------------------------
# Section 7: Read Duplicates
#--------------------------------------------------------------
def read_duplicates(veriout_file_path):
    """
    Read duplicate line data from the verification output file.
    """
    duplicates_info = {}
    try:
        with open(veriout_file_path, 'r', encoding='utf-8') as veriout_file:
            for line in veriout_file:
                if line.startswith("Duplicate line:"):
                    parts = line.split()
                    duplicate_line = ' '.join(parts[2:-3])
                    line_number = parts[-1]
                    duplicates_info.setdefault(duplicate_line, []).append(int(line_number))
        print(f"DEBUG: Successfully read duplicates from {veriout_file_path}.")
    except FileNotFoundError:
        print(f"Error: Verification output file {veriout_file_path} not found.")
    except Exception as e:
        print(f"Error reading duplicates from {veriout_file_path}: {e}")
    finally:
        sys.stdout.flush()  # Ensure logs are updated in real-time
    return duplicates_info

#--------------------------------------------------------------
# End of Section 7: Read Duplicates
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 8: Compare Lines
#--------------------------------------------------------------
def compare_lines(duplicates_info, union_file_path, output_file_path):
    """
    Compare each line in the union file against duplicate info and write findings.
    """
    try:
        with open(union_file_path, 'r', encoding='utf-8') as union_file, \
             open(output_file_path, 'w', encoding='utf-8') as output_file:
            for line in union_file:
                stripped_line = line.strip().lstrip('0').rstrip('0')
                stripped_line = stripped_line.replace('\t', ' ')
                if stripped_line in duplicates_info:
                    output_file.write(f"Duplicate line: {line.strip()} found in union file.\n")
        print(f"DEBUG: Duplicate verification completed. Results written to {output_file_path}.")
    except FileNotFoundError:
        print(f"Error: Required files {union_file_path} or {output_file_path} not found.")
    except Exception as e:
        print(f"Error during line comparison: {e}")
    finally:
        sys.stdout.flush()  # Ensure logs are updated in real-time

#--------------------------------------------------------------
# End of Section 8: Compare Lines
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 9: Clean Duplicates
#--------------------------------------------------------------
def clean_duplicates(verification_output_file_dir, union_file_dir, cleaned_union_file_dir):
    """
    Remove duplicate lines from the union file and output only unique lines.
    """
    verification_output_file = os.path.join(verification_output_file_dir, "veriout.txt")
    union_file_path = os.path.join(union_file_dir, "uniout.txt")
    cleaned_union_file_path = os.path.join(cleaned_union_file_dir, "cleaned_uniout.txt")

    def read_duplicates(verification_output_file):
        duplicates = set()
        try:
            with open(verification_output_file, 'r', encoding='utf-8') as file:
                for line in file:
                    stripped_line = line.strip()
                    if stripped_line and not stripped_line.startswith("No duplicate lines found"):
                        duplicates.add(stripped_line)
            print(f"DEBUG: Duplicates successfully read from {verification_output_file}.")
        except FileNotFoundError:
            print(f"Error: Verification output file {verification_output_file} not found.")
        except Exception as e:
            print(f"Error reading verification file: {e}")
        finally:
            sys.stdout.flush()
        return duplicates

    try:
        # Read duplicate info from the verification output file
        duplicates_info = read_duplicates(verification_output_file)

        # Process the union file to remove duplicates
        unique_lines = set()
        cleaned_lines = []
        with open(union_file_path, 'r', encoding='utf-8') as union_file:
            for line in union_file:
                stripped_line = line.strip().lstrip('0').rstrip('0')
                stripped_line = stripped_line.replace('\t', ' ')
                if stripped_line not in duplicates_info:
                    if stripped_line not in unique_lines:
                        unique_lines.add(stripped_line)
                        cleaned_lines.append(line)

        # Write cleaned data to a new file
        with open(cleaned_union_file_path, 'w', encoding='utf-8') as cleaned_file:
            cleaned_file.writelines(cleaned_lines)
        print(f"DEBUG: Cleaned data written to {cleaned_union_file_path}")

        # Verify if the cleaned file was created successfully
        if os.path.exists(cleaned_union_file_path):
            print(f"{cleaned_union_file_path} has been successfully created.")
        else:
            print(f"Error: {cleaned_union_file_path} was not created.")

    except FileNotFoundError as e:
        print(f"Error: Required file not found: {e}")
    except Exception as e:
        print(f"Error during cleaning process: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 9: Clean Duplicates
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 10: Extract and Verify Duplicates
#--------------------------------------------------------------

async def extract_verify_duplicates(verification_output_file_dir, union_file_dir, results_output_file_dir):
    """
    Extract duplicate lines from the union file and write them to the results output file.
    """
    verification_output_file = os.path.join(verification_output_file_dir, "veriout.txt")
    union_file_path = os.path.join(union_file_dir, "uniout.txt")
    results_output_path = os.path.join(results_output_file_dir, "extracted_duplicates.txt")

    def read_duplicates(verification_output_file):
        duplicates = set()
        try:
            with open(verification_output_file, 'r', encoding='utf-8') as file:
                for line in file:
                    stripped_line = line.strip()
                    if stripped_line and not stripped_line.startswith("No duplicate lines found"):
                        duplicates.add(stripped_line)
                print(f"DEBUG: Duplicates successfully read from {verification_output_file}.")
        except FileNotFoundError:
            print(f"Error: Verification output file {verification_output_file} not found.")
        except Exception as e:
            print(f"Error reading verification file: {e}")
        finally:
            sys.stdout.flush()  # Ensure logs are updated in real-time
        return duplicates

    def compare_lines(duplicates_info, union_file_path, results_output_path):
        try:
            with open(union_file_path, 'r', encoding='utf-8') as union_file:
                union_lines = union_file.readlines()
            
            duplicate_lines = [line for line in union_lines if line.strip() in duplicates_info]
            
            if duplicate_lines:
                with open(results_output_path, 'w', encoding='utf-8') as results_file:
                    results_file.writelines(duplicate_lines)
                print(f"DEBUG: Duplicate lines written to {results_output_path}.")
            else:
                print("DEBUG: No duplicates found during extraction.")
        except FileNotFoundError:
            print(f"Error: Union file {union_file_path} or results output file {results_output_path} not found.")
        except Exception as e:
            print(f"Error extracting duplicates: {e}")
        finally:
            sys.stdout.flush()  # Ensure logs are updated in real-time

    try:
        print(f"DEBUG: Extracting duplicates using verification file: {verification_output_file} and union file: {union_file_path}")
        sys.stdout.flush()

        # Read duplicate info from the verification output file
        duplicates_info = read_duplicates(verification_output_file)

        # Compare the duplicates with the union file and save the results
        compare_lines(duplicates_info, union_file_path, results_output_path)

    except Exception as e:
        print(f"Error during duplicate extraction: {e}")
        raise
    finally:
        sys.stdout.flush()  # Ensure all logs are updated

#--------------------------------------------------------------
# End of Section 10: Extract and Verify Duplicates
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 11: Perform Spot Checks
#--------------------------------------------------------------
def perform_spot_checks(directory_path, num_checks):
    """
    Perform random spot checks on a file from the specified directory to detect duplicates.
    """
    try:
        files = os.listdir(directory_path)
        if not files:
            print("DEBUG: No files found in the directory.")
            sys.stdout.flush()
            return

        print(f"DEBUG: Files found in the directory: {files}")
        sys.stdout.flush()

        file_to_check = None
        for file in files:
            if "uniout.txt" in file:
                file_to_check = os.path.join(directory_path, file)
                break

        if not file_to_check:
            print("DEBUG: Appropriate file for spot checks not found.")
            sys.stdout.flush()
            return

        print(f"DEBUG: Performing spot checks on file: {file_to_check}")
        sys.stdout.flush()

        with open(file_to_check, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        non_empty_lines = [line for line in lines if line.strip()]
        if num_checks > len(non_empty_lines):
            num_checks = len(non_empty_lines)

        # Randomly select sample lines for spot checks
        sample_lines = random.sample(non_empty_lines, num_checks)
        print(f"DEBUG: Sample lines selected for spot checks.")
        sys.stdout.flush()

        duplicates_found = []
        for line in sample_lines:
            if non_empty_lines.count(line) > 1:
                duplicates_found.append(line.strip())

        if duplicates_found:
            print(f"DEBUG: Warning: Duplicates found in spot checks. Total duplicates: {len(duplicates_found)}")
            for dup in duplicates_found:
                print(f"DEBUG: Duplicate line: {dup}")
        else:
            print("DEBUG: No duplicates found in spot checks. Verification successful.")
        sys.stdout.flush()

    except FileNotFoundError:
        print("Error: Cleaned union file not found for spot checks.")
        sys.stdout.flush()
    except Exception as e:
        print(f"Error during spot checks: {e}")
        sys.stdout.flush()
#--------------------------------------------------------------
# End of Section 11: Perform Spot Checks
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 12: Clean Up Files
#--------------------------------------------------------------
def clean_up_files(directory_path):
    """
    Safeguard the cleaned union file, move it to the user's chosen directory if desired,
    and confirm before clearing all files in the working directory.
    """
    try:
        # Check if the cleaned file exists
        cleaned_file_path = os.path.join(directory_path, "cleaned_uniout.txt")
        if os.path.exists(cleaned_file_path):
            print(f"DEBUG: The union file {os.path.basename(cleaned_file_path)} is present.")
            sys.stdout.flush()

            # Ask the user if they want to move the cleaned file
            move_choice = input("Do you want to move it to a different directory? (yes/no): ").strip().lower()
            if move_choice == 'yes':
                target_directory = input("Enter the path for the directory to move the union file to: ").strip()
                if not os.path.exists(target_directory):
                    os.makedirs(target_directory, exist_ok=True)

                # Determine the target file path and handle name conflicts
                base_name, ext = os.path.splitext("cleaned_uniout.txt")
                target_path = os.path.join(target_directory, f"{base_name}{ext}")
                counter = 1
                while os.path.exists(target_path):
                    target_path = os.path.join(target_directory, f"{base_name}_{counter}{ext}")
                    counter += 1

                # Move the file
                shutil.move(cleaned_file_path, target_path)
                print(f"DEBUG: Cleaned file successfully moved to {target_path}.")
            else:
                print("DEBUG: Cleaned file will remain in the working directory.")
            sys.stdout.flush()

        # Ask the user if they want to proceed with clearing the directory
        confirm_clean = input(f"Do you want to proceed with removing all files in {directory_path}? (yes/no): ").strip().lower()
        if confirm_clean == 'yes':
            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            print(f"DEBUG: All files in {directory_path} have been deleted successfully.")
        else:
            print("DEBUG: Operation canceled. No files were deleted.")
        sys.stdout.flush()

    except Exception as e:
        print(f"Error during file clean-up: {e}")
        sys.stdout.flush()
#--------------------------------------------------------------
# End of Section 12: Clean Up Files
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 13: Check Cleaned Duplicates
#--------------------------------------------------------------
import time

def check_cleaned_duplicates(cleaned_file_dir):
    """
    Verify the cleaned union file and output any remaining duplicate lines.
    """
    cleaned_file_path = os.path.join(cleaned_file_dir, "cleaned_uniout.txt")
    
    unique_lines = set()
    duplicate_lines = []
    
    start_time = time.time()
    
    print("DEBUG: Starting duplicate check...")
    sys.stdout.flush()

    try:
        with open(cleaned_file_path, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, start=1):
                stripped_line = line.strip()
                if stripped_line in unique_lines:
                    duplicate_lines.append((line_number, stripped_line))
                else:
                    unique_lines.add(stripped_line)
                
                # Log progress every 1,000,000 lines processed
                if line_number % 1000000 == 0:
                    print(f"DEBUG: Processed {line_number} lines...")
                    sys.stdout.flush()

        end_time = time.time()

        if duplicate_lines:
            print(f"DEBUG: Duplicate lines found in {cleaned_file_path}:")
            for line_number, line in duplicate_lines:
                print(f"DEBUG: Line {line_number}: {line}")
        else:
            print(f"DEBUG: No duplicate lines found in {cleaned_file_path}. Verification successful.")
        
        print(f"DEBUG: Duplicate check completed in {end_time - start_time:.2f} seconds.")
        sys.stdout.flush()

    except FileNotFoundError:
        print(f"Error: Cleaned file {cleaned_file_path} not found.")
        sys.stdout.flush()
    except Exception as e:
        print(f"Error during duplicate check: {e}")
        sys.stdout.flush()
#--------------------------------------------------------------
# End of Section 13: Check Cleaned Duplicates
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 14: File Management and Final Steps
#--------------------------------------------------------------

import time
import gc

def handle_file_management_and_exit(option, working_directory, progress_file, progress_data):
    if option == '7':  # Renaming and moving cleaned file
        cleaned_file_path = os.path.join(working_directory, "cleaned_uniout.txt")
        if os.path.exists(cleaned_file_path):
            print(f"DEBUG: The cleaned file '{os.path.basename(cleaned_file_path)}' is available.")
            sys.stdout.flush()

            # Ask the user if they want to rename the file
            default_file_name = "fusionlogic.txt"
            rename_choice = input(f"Do you want to rename the cleaned file? (Default: '{default_file_name}') (press Enter to use default or type a new name): ").strip()

            if not rename_choice:  # Use default name if no input
                new_file_name = default_file_name
            else:
                new_file_name = rename_choice

            new_cleaned_file_path = os.path.join(working_directory, new_file_name)

            # Ensure the new name does not conflict with an existing file
            if os.path.exists(new_cleaned_file_path):
                print(f"Error: A file with the name '{new_file_name}' already exists. Please try again.")
                sys.stdout.flush()
                return
            else:
                os.rename(cleaned_file_path, new_cleaned_file_path)
                cleaned_file_path = new_cleaned_file_path
                print(f"DEBUG: File renamed to '{new_file_name}'.")
                sys.stdout.flush()

            # Ask the user if they want to move the file
            move_choice = input("Do you want to move the cleaned file to another directory? (yes/no): ").strip().lower()
            if move_choice == 'yes':
                target_directory = input("Enter the target directory path: ").strip()

                # Ensure the target directory exists
                if not os.path.exists(target_directory):
                    os.makedirs(target_directory, exist_ok=True)

                # Move the file to the target directory
                target_file_path = os.path.join(target_directory, os.path.basename(cleaned_file_path))
                shutil.move(cleaned_file_path, target_file_path)
                print(f"DEBUG: File successfully moved to '{target_file_path}'.")
            else:
                print("DEBUG: The cleaned file remains in the working directory.")
            sys.stdout.flush()
        else:
            print("DEBUG: Error: No cleaned file found to rename or move. Please ensure Option 4 has been completed.")
            sys.stdout.flush()

    elif option == '8':  # Clear the working directory
        # Close and forcibly release the log file
        log_file_path = None
        if isinstance(sys.stdout, Logger):
            log_file_path = sys.stdout.log.name  # Capture the log file path
            sys.stdout.log.close()  # Close the log file
            sys.stdout = sys.stdout.terminal  # Reset stdout to the terminal
            print("DEBUG: Log file closed successfully.")
            sys.stdout.flush()

            # Force garbage collection to release file handles completely
            gc.collect()
            time.sleep(0.1)  # Brief pause to allow system-level release of resources

        confirm_reset = input("Are you sure you want to clear the working directory and reset progress? (yes/no): ").strip().lower()
        if confirm_reset == "yes":
            try:
                # Attempt to delete the log file first
                if log_file_path and os.path.exists(log_file_path):
                    try:
                        os.remove(log_file_path)
                        print(f"DEBUG: Log file '{log_file_path}' deleted successfully.")
                    except OSError as e:
                        print(f"DEBUG: Failed to delete log file '{log_file_path}': {e}")
                        print(f"DEBUG: Continuing with cleanup despite failure to delete '{log_file_path}'.")

                # Remove all files and directories within the working directory
                for filename in os.listdir(working_directory):
                    file_path = os.path.join(working_directory, filename)
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)  # Delete file or symbolic link
                            print(f"DEBUG: File '{file_path}' deleted successfully.")
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)  # Delete directory
                            print(f"DEBUG: Directory '{file_path}' deleted successfully.")
                    except Exception as e:
                        print(f"DEBUG: Failed to delete '{file_path}': {e}")

                # Explicitly delete progress.json
                progress_file_path = os.path.join(working_directory, "progress.json")
                if os.path.exists(progress_file_path):
                    try:
                        os.remove(progress_file_path)
                        print("DEBUG: Progress file 'progress.json' has been deleted.")
                    except Exception as e:
                        print(f"DEBUG: Failed to delete progress file: {e}")
                else:
                    print("DEBUG: Progress file not found. Nothing to delete.")

            except Exception as e:
                print(f"Error during file clean-up: {e}")
            finally:
                sys.stdout.flush()

            print("DEBUG: Working directory cleared and progress reset!")
            sys.stdout.flush()
        else:
            print("DEBUG: Operation canceled. No files were deleted.")
            sys.stdout.flush()

    elif option == '9':  # Exit the program
        print("DEBUG: Exiting FusionLogic. Goodbye!")
        sys.stdout.flush()
        exit()

#--------------------------------------------------------------
# End of Section 14: File Management and Final Steps
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 15: Memory Logging and Diagnostics
#--------------------------------------------------------------
import psutil
import gc

def log_memory_usage():
    """
    Logs current memory usage and garbage collection stats.
    """
    try:
        memory = psutil.virtual_memory()
        print(f"DEBUG: Memory Usage - Used: {memory.used // 1024**2} MB, Available: {memory.available // 1024**2} MB")
        print(f"DEBUG: Total Memory: {memory.total // 1024**2} MB, Percentage Used: {memory.percent}%")
        sys.stdout.flush()  # Ensure logs are immediately reflected in the log file
    except Exception as e:
        print(f"Error logging memory usage: {e}")
        sys.stdout.flush()

def log_garbage_collection():
    """
    Logs garbage collection frequency and triggered events.
    """
    try:
        print(f"DEBUG: Garbage Collection Run - Thresholds: {gc.get_threshold()}, Counts: {gc.get_count()}")
        sys.stdout.flush()  # Ensure logs are immediately reflected in the log file
    except Exception as e:
        print(f"Error logging garbage collection: {e}")
        sys.stdout.flush()
#--------------------------------------------------------------
# End of Section 15: Memory Logging and Diagnostics
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 16: Disk-Based Chunking Logic
#--------------------------------------------------------------

import time  # Importing time for performance monitoring

def split_file_into_chunks(file_path, chunk_dir, chunk_size=100000):
    """
    Split a large file into smaller chunks and save them into the specified directory.
    """
    try:
        os.makedirs(chunk_dir, exist_ok=True)  # Ensure the chunk directory exists

        with open(file_path, 'r', encoding='utf-8') as file:
            chunk = []
            chunk_count = 0
            for line in file:
                chunk.append(line.strip())
                if len(chunk) >= chunk_size:
                    chunk_file_path = os.path.join(chunk_dir, f"chunk_{chunk_count + 1}.txt")
                    with open(chunk_file_path, 'w', encoding='utf-8') as chunk_file:
                        chunk_file.writelines(line + '\n' for line in chunk)
                    chunk = []  # Reset chunk
                    chunk_count += 1
                    print(f"DEBUG: Created chunk {chunk_count}: {chunk_file_path}")
                    sys.stdout.flush()

            # Save any remaining lines in a final chunk
            if chunk:
                chunk_file_path = os.path.join(chunk_dir, f"chunk_{chunk_count + 1}.txt")
                with open(chunk_file_path, 'w', encoding='utf-8') as chunk_file:
                    chunk_file.writelines(line + '\n' for line in chunk)
                print(f"DEBUG: Created final chunk: {chunk_file_path}")
                sys.stdout.flush()

    except Exception as e:
        print(f"Error while splitting file into chunks: {e}")
        sys.stdout.flush()

def process_chunk(chunk_file_path, comparison_set, unique_output_path, match_output_path):
    """
    Process a single chunk to identify matches and unique records.
    """
    try:
        matches = []
        unique = []

        with open(chunk_file_path, 'r', encoding='utf-8') as chunk_file:
            for line in chunk_file:
                line = line.strip()
                if line in comparison_set:
                    matches.append(line)
                else:
                    unique.append(line)
                    comparison_set.add(line)

        # Write matches and unique records to their respective output files
        write_matches_as_you_go(match_output_path, matches)
        write_union_as_you_go(unique_output_path, unique)
        print(f"DEBUG: Processed chunk {chunk_file_path}. Matches and unique records written.")
        sys.stdout.flush()

    except Exception as e:
        print(f"Error processing chunk {chunk_file_path}: {e}")
        sys.stdout.flush()

def merge_chunks(chunk_dir, final_output_path):
    """
    Merge all processed chunks into a single final output file.
    """
    try:
        with open(final_output_path, 'w', encoding='utf-8') as final_output:
            for chunk_file in sorted(os.listdir(chunk_dir)):
                chunk_file_path = os.path.join(chunk_dir, chunk_file)
                with open(chunk_file_path, 'r', encoding='utf-8') as chunk:
                    shutil.copyfileobj(chunk, final_output)
        print(f"DEBUG: All chunks successfully merged into {final_output_path}")
        sys.stdout.flush()
    except Exception as e:
        print(f"Error merging chunks: {e}")
        sys.stdout.flush()

def clean_up_chunk_directory(chunk_dir):
    """
    Remove all temporary chunk files and clean the chunk directory.
    """
    try:
        shutil.rmtree(chunk_dir)
        print(f"DEBUG: Cleaned up chunk directory: {chunk_dir}")
        sys.stdout.flush()
    except Exception as e:
        print(f"Error cleaning up chunk directory: {e}")
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 16: Disk-Based Chunking Logic
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 17: Verify and Clean Records
#--------------------------------------------------------------

def verify_and_clean_records():
    """
    Verify and clean records from a specified file by ensuring all lines are unique.
    Output the cleaned records to a new file.
    """
    input_file_path = os.path.join(working_directory, "uniout.txt")  # Example input file
    cleaned_file_path = os.path.join(working_directory, "cleaned_uniout.txt")
    
    unique_lines = set()

    try:
        print(f"DEBUG: Starting verification and cleaning process for file: {input_file_path}")
        with open(input_file_path, 'r', encoding='utf-8') as input_file:
            for line in input_file:
                stripped_line = line.strip()
                unique_lines.add(stripped_line)

        with open(cleaned_file_path, 'w', encoding='utf-8') as output_file:
            output_file.writelines(f"{line}\n" for line in unique_lines)

        print(f"DEBUG: Cleaning complete. Output saved to {cleaned_file_path}")
    except FileNotFoundError:
        print(f"Error: File '{input_file_path}' not found.")
    except Exception as e:
        print(f"Error during verification and cleaning: {e}")
    finally:
        sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 17: Verify and Clean Records
#--------------------------------------------------------------

#--------------------------------------------------------------
# Section 18: Main Menu Logic
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
    if not working_directory:  # First-time setup
        working_directory = input("Enter the path for the working directory: ").strip()
        if not os.path.exists(working_directory):
            os.makedirs(working_directory, exist_ok=True)
        print(f"DEBUG: Working directory set to: {working_directory}")
    else:  # Ask if the user wants to reuse the saved directory
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
        
        # Clear the log file by opening it in write mode and immediately closing it
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            pass  # This empties the file
        
        # Redirect standard output to the log file
        sys.stdout = Logger(log_file_path)
        
        print(f"Logging started. Output will be saved to '{log_file_path}'.\n")
        sys.stdout.flush()
    except Exception as e:
        print(f"DEBUG: Failed to activate logging: {e}")
        raise

class Logger:
    """
    Custom Logger class to write both to console and log file.
    """
    def __init__(self, log_file_path):
        self.log_file = open(log_file_path, "a", encoding="utf-8")
        self.console = sys.__stdout__ or sys.stderr  # Default to sys.stderr if sys.__stdout__ is None

    def write(self, message):
        if self.console:
            self.console.write(message)  # Write to console
        self.log_file.write(message)  # Write to log file

    def flush(self):
        if self.console:
            self.console.flush()
        self.log_file.flush()
def initialize_progress_file(directory):
    """
    Initialize the progress.json file in the specified working directory.
    """
    progress_file = os.path.join(directory, "progress.json")  # Ensure this returns a valid string
    if not os.path.isfile(progress_file):
        # Create a fresh progress file
        progress_data = {"processed_files": {"1": False, "2": False, "3": False, "4": False, "5": False, "6": False}}
        with open(progress_file, 'w', encoding='utf-8') as file:
            json.dump(progress_data, file, indent=4)
        print(f"DEBUG: Progress file created at '{progress_file}'.")
    else:
        print(f"DEBUG: Progress file already exists at '{progress_file}'.")
    sys.stdout.flush()
    return progress_file  # Ensure this returns a valid string and not a tuple

def load_progress(progress_file):
    """
    Load progress data from the progress.json file.
    """
    try:
        if os.path.exists(progress_file):  # Ensure progress_file is correctly passed as a string
            with open(progress_file, 'r', encoding='utf-8') as file:
                return json.load(file)
        else:
            print(f"DEBUG: Progress file '{progress_file}' not found. Starting fresh progress.")
            return {"processed_files": {"1": False, "2": False, "3": False, "4": False, "5": False, "6": False}}
    except Exception as e:
        print(f"DEBUG: Error loading progress file: {e}. Starting fresh progress.")
        return {"processed_files": {"1": False, "2": False, "3": False, "4": False, "5": False, "6": False}}

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

    # File Integrity Check
    if not os.path.isfile(input_file_path):
        print(f"DEBUG: Error: File '{input_file_path}' not found. Please provide a valid file.")
        sys.stdout.flush()
        return

    try:
        # Read and process the file with progress tracking
        unique_records = set()
        with open(input_file_path, 'r', encoding='utf-8') as input_file:
            lines = input_file.readlines()  # Read all lines to determine total count
            print(f"DEBUG: File integrity check passed. Total lines in file: {len(lines)}")
            with tqdm(total=len(lines), desc="Processing lines", unit="line") as pbar, \
                 open(cleaned_output_path, 'w', encoding='utf-8') as cleaned_file:
                for line in lines:
                    normalized_line = normalize_string(line.strip())  # Normalize before comparison
                    if normalized_line not in unique_records:
                        cleaned_file.write(line)
                        unique_records.add(normalized_line)
                    pbar.update(1)  # Update progress bar

        print(f"DEBUG: Cleaned file '{cleaned_output_path}' successfully created.")
        print(f"DEBUG: Total lines processed: {len(lines)}")
        print(f"DEBUG: Unique lines saved: {len(unique_records)}")
        sys.stdout.flush()

        # Update progress
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

        # Update progress
        progress_data["processed_files"]["2"] = True
        save_progress(progress_data, progress_file)

        # Automatically handle re-cleaning using enhanced normalization
        if duplicates_found:
            confirmation = input("Duplicates were found. Would you like to rerun Verify and Clean Records? (yes/no): ").strip().lower()
            if confirmation == "yes":
                print("DEBUG: Rerunning Verify and Clean Records with normalization...")
                try:
                    # Process the cleaned file with normalization to remove duplicates
                    normalized_records = set()
                    with open(cleaned_file_path, 'r', encoding='utf-8') as file:
                        for line in file:
                            normalized_line = normalize_string(line.strip())
                            normalized_records.add(normalized_line)
                    with open(cleaned_file_path, 'w', encoding='utf-8') as file:
                        for record in normalized_records:
                            file.write(record + '\n')
                    print("DEBUG: Duplicate cleanup successfully completed after normalization!")
                except Exception as e:
                    print(f"DEBUG: Error during re-cleaning process: {e}")
                sys.stdout.flush()
            else:
                print("DEBUG: Cleaning process was not rerun.")
                sys.stdout.flush()

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

        # Perform random sampling
        sampled_lines = random.sample(lines, min(num_checks, len(lines)))  # Ensure number of samples doesn't exceed file length
        print("\nDEBUG: Spot Check Results:")
        for i, line in enumerate(sampled_lines, start=1):
            print(f"Sample {i}: {line.strip()}")

        print("DEBUG: Spot checks completed successfully!")
        sys.stdout.flush()

        # Update progress
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
                if search_string in normalize_string(line.strip()):  # Normalizing during search
                    matches.append((line_number, line.strip()))

        results_output_file = os.path.join(working_directory, "search_results.txt")
        with open(results_output_file, 'w', encoding='utf-8') as results_file:
            if matches:
                for match in matches:
                    results_file.write(f"Line {match[0]}: {match[1]}\n")
                print(f"DEBUG: Search results saved to '{results_output_file}'.")
            else:
                results_file.write("No matches found.\n")
                print(f"DEBUG: No matches found.")

        sys.stdout.flush()
    except Exception as e:
        print(f"DEBUG: Error during string search: {e}")
        sys.stdout.flush()
        
def rename_and_move_file(progress_data, progress_file):
    """
    Rename and optionally move the cleaned file to a user-specified directory, while updating progress.
    """
    print("DEBUG: Starting rename and move operation...")
    sys.stdout.flush()

    # Updated default file name
    default_file_name = "deduplicatorlogic-s.txt"
    cleaned_file_path = os.path.join(working_directory, "deduplicatorlogic.txt")

    if not os.path.isfile(cleaned_file_path):
        print(f"DEBUG: Error: File '{cleaned_file_path}' not found. Please run Option 1 first.")
        sys.stdout.flush()
        return

    try:
        # Prompt the user for the new directory and file name
        new_directory = input("Enter the path to the directory where you want to move the file: ").strip()
        new_file_name = input(f"Enter the new name for the cleaned file (default: {default_file_name}): ").strip()

        # Fallback to updated default name if the user does not enter a new name
        if not new_file_name:
            new_file_name = default_file_name

        # Handle naming conflict (increment name if file exists)
        new_file_path = os.path.join(new_directory, new_file_name)
        counter = 1
        while os.path.exists(new_file_path):
            name, ext = os.path.splitext(new_file_name)
            new_file_path = os.path.join(new_directory, f"{name}({counter}){ext}")
            counter += 1

        # Ensure the new directory exists
        if not os.path.exists(new_directory):
            os.makedirs(new_directory, exist_ok=True)

        # Rename and move the file
        shutil.move(cleaned_file_path, new_file_path)
        print(f"DEBUG: File successfully moved and renamed to: {new_file_path}")
        sys.stdout.flush()

        # Update progress
        progress_data["processed_files"]["5"] = True  # Mark Option 5 as completed
        save_progress(progress_data, progress_file)
    except Exception as e:
        print(f"DEBUG: Error during renaming/moving process: {e}")
        sys.stdout.flush()

def clear_working_directory(progress_data, progress_file):
    """
    Fully reset the working directory by deleting all files except for logfile.txt.
    Warns the user if the deduplicator file has not been moved.
    """
    print("DEBUG: Clearing the working directory...")
    sys.stdout.flush()

    # Check if the deduplicator file has been moved
    deduplicator_file_path = os.path.join(working_directory, "deduplicatorlogic.txt")
    if os.path.exists(deduplicator_file_path):
        print("WARNING: The deduplicator file has not been moved. Do you still wish to proceed?")
        confirmation = input("This action will permanently delete the deduplicator file. Continue? (yes/no): ").strip().lower()
        if confirmation != "yes":
            print("DEBUG: Clearing operation aborted by the user.")
            sys.stdout.flush()
            return

    # Prompt for confirmation
    confirmation = input("Are you sure you want to clear the working directory? This action cannot be undone. (yes/no): ").strip().lower()
    if confirmation != "yes":
        print("DEBUG: Clearing operation aborted by the user.")
        sys.stdout.flush()
        return

    try:
        # Remove all files except logfile.txt
        files = [f for f in os.listdir(working_directory) if f != "logfile.txt"]
        for file in files:
            file_path = os.path.join(working_directory, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

        print("DEBUG: Working directory cleared successfully, except for 'logfile.txt'.")
        sys.stdout.flush()

        # Inform the user that they should exit the program
        print("NOTE: The program has been reset. Please exit and restart the program to continue.")
        sys.stdout.flush()

    except Exception as e:
        print(f"DEBUG: Error during clearing process: {e}")
        sys.stdout.flush()
if __name__ == "__main__":
    print("=" * 60)
    print("Welcome to FusionLogic Deduplicator Search Version!")
    print("Advanced Deduplication Tool")
    print("Efficiently clean, validate, and verify datasets.")
    print("=" * 60)
    sys.stdout.flush()

    # Set up the working directory
    working_directory = get_working_directory()

    # Activate logging
    activate_logging()

    # Initialize progress file
    progress_file = initialize_progress_file(working_directory)

    # Load progress data
    progress_data = load_progress(progress_file)

    # Main menu loop begins here...
    while True:
        print("\nMain Menu:")
        print("1. Verify and Clean Records (Mandatory Step)")
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
        elif option in ['2', '3', '4', '5', '6']:
            if progress_data["processed_files"]["1"]:
                if option == '2':
                    extract_duplicates(progress_data, progress_file)
                elif option == '3':
                    perform_spot_checks(progress_data, progress_file)
                elif option == '4':
                    search_string_in_file()
                elif option == '5':
                    rename_and_move_file(progress_data, progress_file)
                elif option == '6':
                    clear_working_directory(progress_data, progress_file)
            else:
                print("DEBUG: Error: Option 1 must be completed first to create the deduplicated file.")
                sys.stdout.flush()
        elif option == '7':  
            print("DEBUG: Exiting the program. Goodbye!")
            sys.stdout.flush()
            break
        else:
            print("DEBUG: Invalid choice. Please select a valid menu option.")
            sys.stdout.flush()

#--------------------------------------------------------------
# End of Section 18: Main Menu Logic
#--------------------------------------------------------------

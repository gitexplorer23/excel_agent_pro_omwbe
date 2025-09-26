import csv
import re
import os
import sys
from pathlib import Path

# ============================================================================
# CONFIGURATION - SET YOUR FOLDER PATH HERE
# ============================================================================
input_folder_path = r"C:\Users\osmelc\OneDrive - Washington State Department of Transportation\OMWBE IT Team Work - PPP Team Channel - PPP Team Channel\Analytics\Data_Sources\Higher_ed\WWU"

# Optional: Set output folder path (leave as None to use same folder as input)
output_folder_path = r"C:\Users\osmelc\OneDrive - Washington State Department of Transportation\OMWBE IT Team Work - PPP Team Channel - PPP Team Channel\Analytics\Data_Sources\Higher_ed\WWU\Converted_Files"
# output_folder_path = None  # Will save in same folder as input files

# File pattern to match (leave as None to process ALL .txt files)
file_pattern = "*.txt"  # or "WWU Spend*.txt" to be more specific
# ============================================================================

def parse_fixed_width_line(line):
    """Parse a single line of fixed-width data into components"""
    line = line.strip()
    
    if not line:
        return None
    
    # Report Part: First digit
    report_part = line[0] if line else ""
    
    # College Number: Next 4 digits
    college_number = line[1:5] if len(line) > 4 else ""
    
    # Find 9-digit federal ID
    fed_id_match = re.search(r'(\d{9})(?=\s*[A-Z]{2})', line[5:])
    
    if not fed_id_match:
        fed_id_match = re.search(r'(\d{9})', line[5:])
        if not fed_id_match:
            return None
    
    # Adjust position since we started search from position 5
    fed_id_start = fed_id_match.start(1) + 5
    fed_id_end = fed_id_match.end(1) + 5
    fed_id = fed_id_match.group(1)
    
    # Firm name is between college number and federal ID
    firm_name = line[5:fed_id_start].strip()
    
    # After federal ID, find sub-object (2 letters)
    after_fed_id = line[fed_id_end:].strip()
    sub_object_match = re.match(r'\s*([A-Z]{2})', after_fed_id)
    
    if not sub_object_match:
        sub_object_match = re.search(r'([A-Z]{2})', after_fed_id)
        if not sub_object_match:
            return None
    
    sub_object = sub_object_match.group(1)
    
    # Dollar amount
    dollar_match = re.search(r'(\d+\.\d{2})', after_fed_id)
    if not dollar_match:
        dollar_match = re.search(r'(\d{5,})', after_fed_id)
        if dollar_match:
            amount = dollar_match.group(1)
            if len(amount) >= 3:
                dollar_amount = f"{amount[:-2]}.{amount[-2:]}"
            else:
                dollar_amount = f"0.{amount.zfill(2)}"
        else:
            return None
    else:
        dollar_amount = dollar_match.group(1)
    
    # Fiscal year - extract first 2 digits from 4-digit year
    fiscal_year_match = re.search(r'(\d{4})$', line)
    if fiscal_year_match:
        full_year = fiscal_year_match.group(1)
        fiscal_year = full_year[:2]
    else:
        fiscal_year = ""
    
    # Fiscal month
    fiscal_month = "13"
    
    return {
        'Report Part': report_part,
        'College Number': college_number,
        'Firm Name': firm_name,
        'Firm Fed ID': fed_id,
        'Sub-object': sub_object,
        'Dollar Amount': dollar_amount,
        'Fiscal Year': fiscal_year,
        'Fiscal Month': fiscal_month
    }

def convert_single_file(input_file_path, output_file_path):
    """Convert a single fixed-width file to CSV format"""
    headers = ['Report Part', 'College Number', 'Firm Name', 'Firm Fed ID', 
               'Sub-object', 'Dollar Amount', 'Fiscal Year', 'Fiscal Month']
    
    try:
        with open(input_file_path, 'r', encoding='utf-8') as infile:
            with open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=headers)
                writer.writeheader()
                
                line_count = 0
                processed_count = 0
                
                for line in infile:
                    line_count += 1
                    parsed_data = parse_fixed_width_line(line)
                    
                    if parsed_data:
                        writer.writerow(parsed_data)
                        processed_count += 1
                    elif line.strip():
                        print(f"  Warning: Could not parse line {line_count}")
                
                return processed_count, line_count
    
    except Exception as e:
        print(f"  Error processing file: {e}")
        return 0, 0

def process_folder():
    """Process all files in the specified folder"""
    
    # Validate input folder
    if not os.path.exists(input_folder_path):
        print(f"Error: Input folder not found: {input_folder_path}")
        return
    
    # Create output folder if specified and doesn't exist
    if output_folder_path:
        os.makedirs(output_folder_path, exist_ok=True)
        output_dir = output_folder_path
    else:
        output_dir = input_folder_path
    
    # Find all matching files
    input_folder = Path(input_folder_path)
    matching_files = list(input_folder.glob(file_pattern))
    
    if not matching_files:
        print(f"No files found matching pattern '{file_pattern}' in {input_folder_path}")
        return
    
    print(f"Found {len(matching_files)} files to process:")
    for file in matching_files:
        print(f"  - {file.name}")
    
    print("\nProcessing files...")
    print("=" * 50)
    
    total_files_processed = 0
    total_lines_processed = 0
    total_lines_found = 0
    
    for input_file in matching_files:
        # Generate output filename
        output_filename = input_file.stem + "_converted.csv"
        output_file_path = os.path.join(output_dir, output_filename)
        
        print(f"\nProcessing: {input_file.name}")
        print(f"Output: {output_filename}")
        
        # Convert the file
        processed_lines, total_lines = convert_single_file(str(input_file), output_file_path)
        
        if processed_lines > 0:
            print(f"  ✅ Success: {processed_lines}/{total_lines} lines processed")
            total_files_processed += 1
            total_lines_processed += processed_lines
            total_lines_found += total_lines
        else:
            print(f"  ❌ Failed to process file")
    
    print("\n" + "=" * 50)
    print("BATCH PROCESSING COMPLETE!")
    print(f"Files processed: {total_files_processed}/{len(matching_files)}")
    print(f"Total lines processed: {total_lines_processed}/{total_lines_found}")
    print(f"Output folder: {output_dir}")

def main():
    """Main function"""
    print("Fixed-Width to CSV Batch Converter")
    print("=" * 40)
    print(f"Input folder: {input_folder_path}")
    print(f"File pattern: {file_pattern}")
    print(f"Output folder: {output_folder_path if output_folder_path else 'Same as input'}")
    print()
    
    # Ask for confirmation
    response = input("Proceed with batch conversion? (y/n): ").lower().strip()
    if response != 'y':
        print("Conversion cancelled.")
        return
    
    process_folder()

if __name__ == "__main__":
    main()
import csv
import re
import os
import sys

# ============================================================================
# CONFIGURATION - SET YOUR FILE PATH HERE
# ============================================================================
text_file_path = r"C:\Users\osmelc\OneDrive - Washington State Department of Transportation\OMWBE IT Team Work - PPP Team Channel - PPP Team Channel\Analytics\Data_Sources\Higher_ed\WWU\All in one .txt"
output_csv_path = r"C:\Users\osmelc\OneDrive - Washington State Department of Transportation\OMWBE IT Team Work - PPP Team Channel - PPP Team Channel\Analytics\Data_Sources\Higher_ed\WWU\converted_data.csv"
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

def convert_file_to_csv(input_file_path, output_file_path=None):
    """Convert fixed-width file to CSV format"""
    if not os.path.exists(input_file_path):
        print("Error: File not found.")
        return False
    
    if output_file_path is None:
        base_name = os.path.splitext(input_file_path)[0]
        output_file_path = f"{base_name}_converted.csv"
    
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
                        print(f"Warning: Could not parse line {line_count}")
                
                print("Conversion complete!")
                print(f"Input file: {input_file_path}")
                print(f"Output file: {output_file_path}")
                print(f"Lines processed: {processed_count}/{line_count}")
                
        return True
    
    except Exception as e:
        print(f"Error processing file: {e}")
        return False

def main():
    """Main function"""
    if 'text_file_path' in globals() and text_file_path:
        input_file = text_file_path
        output_file = output_csv_path
        print(f"Using configured file path: {input_file}")
    elif len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        input_file = input("Enter the path to your input file: ").strip()
        output_file = input("Enter output CSV file path (press Enter for auto-generate): ").strip()
        
        if not output_file:
            output_file = None
    
    input_file = input_file.strip('"\'')
    if output_file:
        output_file = output_file.strip('"\'')
    
    success = convert_file_to_csv(input_file, output_file)
    
    if success:
        print("File conversion completed successfully!")
    else:
        print("File conversion failed.")

if __name__ == "__main__":
    main()
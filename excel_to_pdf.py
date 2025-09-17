import os
import pandas as pd
from openpyxl import load_workbook
from openpyxl.drawing.image import Image
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
from dotenv import load_dotenv
import logging
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_directories(base_path, analysts):
    """Create directories for each analyst if they don't exist"""
    directories = {}
    for analyst in analysts:
        analyst_path = os.path.join(base_path, analyst.title())
        Path(analyst_path).mkdir(parents=True, exist_ok=True)
        directories[analyst] = analyst_path
        logger.info(f"Directory ready: {analyst_path}")
    return directories

def excel_to_pdf_simple(excel_file_path, sheet_name, output_pdf_path):
    """Convert Excel sheet to PDF using pandas and matplotlib (simple tables)"""
    try:
        # Read the Excel sheet
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
        
        # Create PDF
        with PdfPages(output_pdf_path) as pdf:
            fig, ax = plt.subplots(figsize=(11.69, 8.27))  # A4 landscape
            ax.axis('tight')
            ax.axis('off')
            
            # Create table
            table = ax.table(cellText=df.values, 
                           colLabels=df.columns,
                           cellLoc='center',
                           loc='center')
            
            # Style the table
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(1.2, 1.5)
            
            # Style header
            for i in range(len(df.columns)):
                table[(0, i)].set_facecolor('#40466e')
                table[(0, i)].set_text_props(weight='bold', color='white')
            
            plt.title(f'Analysis Report - {sheet_name.title()}', fontsize=16, fontweight='bold', pad=20)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
        logger.info(f"Successfully created PDF: {output_pdf_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error converting {sheet_name} to PDF: {str(e)}")
        return False

def excel_to_pdf_advanced(excel_file_path, sheet_name, output_pdf_path):
    """Convert Excel sheet to PDF preserving formatting (requires win32com - Windows only)"""
    try:
        import win32com.client as win32
        
        # Convert to absolute path
        excel_file_path = os.path.abspath(excel_file_path)
        output_pdf_path = os.path.abspath(output_pdf_path)
        
        # Open Excel application
        excel_app = win32.Dispatch("Excel.Application")
        excel_app.Visible = False
        excel_app.DisplayAlerts = False
        
        # Open workbook
        workbook = excel_app.Workbooks.Open(excel_file_path)
        
        # Select the specific sheet
        worksheet = workbook.Sheets(sheet_name)
        
        # Export to PDF
        worksheet.ExportAsFixedFormat(0, output_pdf_path)
        
        # Close workbook and quit Excel
        workbook.Close(False)
        excel_app.Quit()
        
        logger.info(f"Successfully created PDF with formatting: {output_pdf_path}")
        return True
        
    except ImportError:
        logger.warning("win32com not available. Using simple PDF conversion method.")
        return excel_to_pdf_simple(excel_file_path, sheet_name, output_pdf_path)
    except Exception as e:
        logger.error(f"Error with advanced PDF conversion for {sheet_name}: {str(e)}")
        # Fallback to simple method
        return excel_to_pdf_simple(excel_file_path, sheet_name, output_pdf_path)

def main():
    # Get environment variables
    excel_file_path = os.getenv('EXCEL_FILE_PATH_PDF')
    sheet_names = os.getenv('SHEET_NAMES_PDF', '').split(',')
    base_pdf_path = os.getenv('SHEET_CONVERTED_PDF_PATH')
    
    # Validate environment variables
    if not excel_file_path or not os.path.exists(excel_file_path):
        logger.error("Excel file path not found or doesn't exist")
        return
    
    if not sheet_names or sheet_names == ['']:
        logger.error("No sheet names provided")
        return
    
    if not base_pdf_path:
        logger.error("PDF output path not provided")
        return
    
    # Clean sheet names (remove whitespace)
    sheet_names = [name.strip() for name in sheet_names if name.strip()]
    
    logger.info(f"Processing Excel file: {excel_file_path}")
    logger.info(f"Sheets to convert: {sheet_names}")
    logger.info(f"Output base path: {base_pdf_path}")
    
    # Setup directories for each analyst
    directories = setup_directories(base_pdf_path, sheet_names)
    
    # Load the Excel file to check available sheets
    try:
        excel_file = pd.ExcelFile(excel_file_path)
        available_sheets = excel_file.sheet_names
        logger.info(f"Available sheets in Excel file: {available_sheets}")
    except Exception as e:
        logger.error(f"Error reading Excel file: {str(e)}")
        return
    
    # Process each sheet
    success_count = 0
    total_count = len(sheet_names)
    
    for sheet_name in sheet_names:
        if sheet_name not in available_sheets:
            logger.warning(f"Sheet '{sheet_name}' not found in Excel file. Skipping.")
            continue
            
        # Create output PDF path
        analyst_folder = directories[sheet_name]
        pdf_filename = f"{sheet_name}_analysis_report.pdf"
        output_pdf_path = os.path.join(analyst_folder, pdf_filename)
        
        logger.info(f"Converting sheet '{sheet_name}' to PDF...")
        
        # Try advanced method first (Windows with Excel), fallback to simple method
        if excel_to_pdf_advanced(excel_file_path, sheet_name, output_pdf_path):
            success_count += 1
    
    # Summary
    logger.info(f"Conversion complete: {success_count}/{total_count} sheets successfully converted to PDF")
    if success_count == total_count:
        logger.info("All conversions successful!")
    else:
        logger.warning(f"{total_count - success_count} conversions failed or were skipped")

if __name__ == "__main__":
    main()
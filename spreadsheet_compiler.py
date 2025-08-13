import pandas as pd
import os
import shutil
from pathlib import Path
from datetime import datetime


def clean_dataframe(df, filename):
    """
    Cleans the dataframe by removing header and metadata rows from Monthly Billing Summary files.
    Keeps only rows that contain actual data.
    
    Args:
        df (pandas.DataFrame): The original dataframe
        filename (str): Name of the source file for context
    
    Returns:
        pandas.DataFrame: Cleaned dataframe with only data rows
    """
    if df.empty:
        return df
    
    # Look for the row that contains actual column headers (starts with standard business columns)
    # You can customize the header indicators based on your specific use case
    header_indicators = [
        'CUSTOMER_INVOICE_NUMBER', 'ID', 'INVOICE', 'CUSTOMER', 'BILLING',
        'CURRENCY', 'DATE', 'AMOUNT', 'FEE', 'CHARGE'
    ]
    
    header_row_idx = None
    
    # Search for the header row
    for idx, row in df.iterrows():
        row_str = ' '.join(str(cell).upper() for cell in row.values if pd.notna(cell))
        if any(indicator in row_str for indicator in header_indicators):
            # Check if this looks like a header row (contains multiple header indicators)
            indicators_found = sum(1 for indicator in header_indicators if indicator in row_str)
            if indicators_found >= 2:  # At least 2 header indicators
                header_row_idx = idx
                break
    
    if header_row_idx is not None:
        # Use this row as the new header
        new_header = df.iloc[header_row_idx].values
        
        # Get data starting from the next row
        data_df = df.iloc[header_row_idx + 1:].copy()
        
        # Set the new column names
        data_df.columns = new_header
        
        # Remove rows that are completely empty or contain only metadata
        data_df = data_df.dropna(how='all')
        
        # Filter out rows that look like headers or metadata (e.g., rows with text like "Monthly Billing Summary")
        if not data_df.empty:
            # Remove rows where the first column contains metadata keywords
            # You can add your own keywords to this list
            metadata_keywords = [
                'Monthly Billing Summary', 'Report', 'TOTAL', 'Summary', 
                'Generated', 'Period', 'Currency', 'Page'
            ]
            
            first_col = data_df.columns[0]
            if first_col in data_df.columns:
                mask = data_df[first_col].astype(str).str.upper().str.contains(
                    '|'.join(metadata_keywords), na=False, regex=True
                )
                data_df = data_df[~mask]
        
        print(f"  - Found header at row {header_row_idx + 1}, extracted {len(data_df)} data rows")
        return data_df
    else:
        # If no clear header found, assume the dataframe is already clean
        # But still remove obvious metadata rows
        clean_df = df.copy()
        
    
        # Remove rows containing metadata keywords in any column
        # You can add your own custom words for this
        metadata_keywords = [
            'Monthly Billing Summary', 'Report', 'Generated on', 'Period:', 'Currency:'
        ]
        
        for keyword in metadata_keywords:
            keyword_upper = keyword.upper()  # Capture the value
            mask = clean_df.astype(str).apply(
                lambda x, kw=keyword_upper: x.str.upper().str.contains(kw, na=False)
            ).any(axis=1)
            clean_df = clean_df[~mask]
        
        # Remove rows that are mostly empty (more than 80% NaN)
        clean_df = clean_df.dropna(thresh=len(clean_df.columns) * 0.2)
        
        print(f"  - No clear header found, applied general cleaning: {len(clean_df)} rows remaining")
        return clean_df


def compile_xlsx_to_csv(ready_folder="Spreadsheet/ready", 
                        done_folder="Spreadsheet/done", 
                        output_folder="CSV",
                        output_filename=None):
    """
    Reads all XLSX files from the ready folder, combines their data into a single CSV file,
    and moves processed files to the done folder.
    
    Args:
        ready_folder (str): Path to folder containing XLSX files to process
        done_folder (str): Path to folder where processed files will be moved
        output_folder (str): Path to folder where CSV output will be saved
        output_filename (str): Name of output CSV file (if None, generates timestamp-based name)
    
    Returns:
        str: Path to the created CSV file, or None if no files were processed
    """
    
    # Convert to Path objects for easier manipulation
    ready_path = Path(ready_folder)
    done_path = Path(done_folder)
    output_path = Path(output_folder)
    
    # Create directories if they don't exist
    ready_path.mkdir(parents=True, exist_ok=True)
    done_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all XLSX files in the ready folder
    xlsx_files = list(ready_path.glob("*.xlsx"))
    
    if not xlsx_files:
        print("No XLSX files found in the ready folder.")
        return None
    
    print(f"Found {len(xlsx_files)} XLSX files to process:")
    for file in xlsx_files:
        print(f"  - {file.name}")
    
    # List to store all dataframes
    all_dataframes = []
    processed_files = []
    
    # Process each XLSX file
    for xlsx_file in xlsx_files:
        try:
            print(f"\nProcessing: {xlsx_file.name}")
            
            # Read the XLSX file
            df = pd.read_excel(xlsx_file)
            
            # Filter out header and metadata rows (COMMENTED OUT TO PRESERVE HEADERS)
            # df_cleaned = clean_dataframe(df, xlsx_file.name)
            
            # Use original dataframe without cleaning
            df_cleaned = df
            
            if df_cleaned is not None and not df_cleaned.empty:
                # Add source file information (optional)
                df_cleaned['source_file'] = xlsx_file.name
                
                all_dataframes.append(df_cleaned)
                processed_files.append(xlsx_file)
                
                print(f"  - Successfully read {len(df_cleaned)} rows (ALL data preserved)")
                # print(f"  - Successfully read {len(df_cleaned)} data rows (filtered from {len(df)} total rows)")
            else:
                print(f"  - No valid data found in {xlsx_file.name} after filtering")
            
        except Exception as e:
            print(f"  - Error processing {xlsx_file.name}: {str(e)}")
            continue
    
    if not all_dataframes:
        print("No files were successfully processed.")
        return None
    
    # Combine all dataframes
    print(f"\nCombining {len(all_dataframes)} dataframes...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Generate output filename if not provided
    if output_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"combined_data_{timestamp}.csv"
    
    # Ensure the filename has .csv extension
    if not output_filename.endswith('.csv'):
        output_filename += '.csv'
    
    # Create full output path
    output_file_path = output_path / output_filename
    
    # Save to CSV
    combined_df.to_csv(output_file_path, index=False)
    print(f"Combined data saved to: {output_file_path}")
    print(f"Total rows in combined file: {len(combined_df)}")
    print(f"Columns: {list(combined_df.columns)}")
    
    # Move processed files to done folder
    print("\nMoving processed files to done folder...")
    for xlsx_file in processed_files:
        try:
            destination = done_path / xlsx_file.name
            shutil.move(str(xlsx_file), str(destination))
            print(f"  - Moved: {xlsx_file.name}")
        except Exception as e:
            print(f"  - Error moving {xlsx_file.name}: {str(e)}")
    
    return str(output_file_path)


def main():
    """
    Main function to run the spreadsheet compiler
    """
    print("=" * 60)
    print("XLSX to CSV Compiler")
    print("=" * 60)
    
    # Run the compilation
    result = compile_xlsx_to_csv()
    
    if result:
        print("\n✅ Process completed successfully!")
    
        print(f"📄 Output file: {result}")
    else:
        print("\n❌ No files were processed.")
    
    print("=" * 60)


if __name__ == "__main__":
    main()

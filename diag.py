import pandas as pd
import json
import sys
import os
from io import BytesIO

def clean_sample_value(value, max_str_length=300):
    """
    Recursively processes a sample value to handle complex types (bytes, nested JSON, long strings).
    Replaces binary data with a size marker and truncates long strings.
    """
    notes = []
    
    if isinstance(value, bytes):
        # 1. Handle Binary Data (Crucial Fix)
        notes.append("This column contains binary data.")
        return f"<Binary Data: {len(value)} bytes>", notes
    
    if isinstance(value, dict):
        # 2. Handle Nested Dictionaries (Recurse)
        new_dict = {}
        for k, v in value.items():
            cleaned_v, v_notes = clean_sample_value(v, max_str_length)
            new_dict[k] = cleaned_v
            notes.extend(v_notes)
        return new_dict, notes
    
    if isinstance(value, list):
        # 3. Handle Nested Lists (Recurse on the first element only to save time/tokens)
        if value:
            cleaned_item, item_notes = clean_sample_value(value[0], max_str_length)
            notes.append(f"List structure. Sampled first element: {cleaned_item}")
            notes.extend(item_notes)
            return f"<List, First Item Type: {type(value[0]).__name__}, Total Items: {len(value)}>", notes
        return "<Empty List>", notes

    if isinstance(value, str):
        # 4. Handle JSON Strings embedded in string fields
        try:
            parsed_json = json.loads(value)
            cleaned_json, json_notes = clean_sample_value(parsed_json, max_str_length)
            notes.append("Value parsed as a nested JSON string.")
            notes.extend(json_notes)
            return cleaned_json, notes
        except (json.JSONDecodeError, TypeError):
            # 5. Truncate Long Text Fields
            if len(value) > max_str_length:
                notes.append("Long text field (sample truncated).")
                return value[:max_str_length] + "...", notes
            return value, notes
    
    # 6. Handle standard types (ints, floats, bools, etc.)
    return value, notes

def inspect_parquet_file(file_path: str):
    """
    Inspects a Parquet file, extracting structural metadata (column names, types) 
    and sample data from the first row of every column. Outputs a structured JSON object.

    Args:
        file_path (str): The path to the Parquet file.

    Returns:
        dict: A dictionary containing the file's structured metadata.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'", file=sys.stderr)
        sys.exit(1)

    df_full = None
    try:
        # Read the full DataFrame to get the correct schema and row count universally
        df_full = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error: Could not read Parquet file '{file_path}'. Reason: {e}", file=sys.stderr)
        sys.exit(1)

    # --- 1. Basic File Metadata ---
    metadata = {
        "file_name": os.path.basename(file_path),
        "file_path": file_path,
        "total_columns": len(df_full.columns),
        # Total rows is guaranteed to be correct here since we read the full DataFrame
        "total_rows": df_full.shape[0], 
        "schema_details": []
    }

    # Use the highly compatible .head(1) slice for sampling
    df_sample = df_full.head(1)
    is_data_present = metadata["total_rows"] > 0
    
    # --- 2. Detailed Column Inspection ---
    for col_name, dtype in df_sample.dtypes.items():
        column_info = {
            "column_name": col_name,
            "pandas_dtype": str(dtype),
            "sample_value": None,
            "python_type": None,
            "notes": ""
        }
        
        if is_data_present:
            # We use iloc[0] on the single-row sample DataFrame
            sample_value = df_sample.iloc[0][col_name] 
            
            column_info["python_type"] = str(type(sample_value).__name__)
            
            # Use the cleaner function for robust, LLM-ready sampling
            cleaned_value, notes = clean_sample_value(sample_value)
            
            column_info["sample_value"] = cleaned_value
            column_info["notes"] = "; ".join(notes)
            if not column_info["notes"]:
                column_info["notes"] = "Simple data type or no special processing needed."
        
        metadata["schema_details"].append(column_info)

    return metadata

if __name__ == "__main__":
    # --- Usage Instructions ---
    # 1. Ensure you have pandas and pyarrow installed: pip install pandas pyarrow
    # 2. Run from command line: python parquet_inspector.py <path_to_your_file.parquet>

    if len(sys.argv) < 2:
        print("\nUsage: python parquet_inspector.py <path_to_parquet_file>", file=sys.stderr)
        print("Example: python parquet_inspector.py data/invoices.parquet", file=sys.stderr)
        sys.exit(1)

    file_to_inspect = sys.argv[1]
    
    # Run the inspection and get the structured dictionary
    result_data = inspect_parquet_file(file_to_inspect)

    # --- Output Generation ---
    # 1. Human-Readable Output (Pretty-printed JSON to console)
    print("\n" * 2)
    print("=" * 70)
    print("        ✅ LLM-Ready & HUMAN-READABLE STRUCTURED METADATA ✅")
    print("=" * 70)
    print(json.dumps(result_data, indent=4, default=str)) # Use default=str for any un-serializable types
    print("\n" * 2)
    
    # 2. LLM-Suitable Data (Optional: A condensed summary for prompt injection)
    # The full JSON above is already perfect for LLMs, but this is a quick summary.
    print("=" * 70)
    print("         🧠 QUICK SUMMARY FOR AI PROMPTS 🧠")
    print("=" * 70)
    
    print(f"File: {result_data['file_name']}")
    print(f"Rows: {result_data['total_rows']}, Cols: {result_data['total_columns']}")
    print("\n--- Column Definitions ---")
    
    for item in result_data['schema_details']:
        sample_display = item['sample_value']
        notes = item['notes']
        
        # Display sample for simple types or use notes for complex ones
        if isinstance(sample_display, str) and sample_display.startswith("<Binary Data:"):
            sample_display = sample_display

        elif isinstance(sample_display, dict):
            sample_display = "<Nested Dict>"
        
        elif isinstance(sample_display, str) and len(sample_display) > 80:
             sample_display = sample_display[:80] + "..."

        elif isinstance(sample_display, list):
             # Handled in cleaner now, but defensive check
             sample_display = f"<List, length={len(sample_display)}>" 

        print(f"| {item['column_name']:<20} | DType: {item['pandas_dtype']:<10} | Sample: {sample_display} | Notes: {notes}")

    print("=" * 70)

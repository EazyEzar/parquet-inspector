# **🔍 Generalized Parquet File Inspector**

## **Project Overview**

This Python utility provides a robust, memory-efficient way to inspect the structure and sample data of any Parquet file without needing to load the entire dataset. It is specifically designed to generate clean, structured JSON metadata that is highly suitable for **Large Language Models (LLMs)**. This makes it an ideal pre-processing tool for data science, MLOps, and automation workflows when dealing with complex, large data dumps.

## **✨ Key Features**

* **LLM-Ready Output:** Generates clean, structured JSON detailing the file's schema.  
* **Robust Sampling:** Loads only the first row to determine column types and extract samples, ensuring **low memory usage** on massive files.  
* **Complex Data Handling:** Recursively cleans and sanitizes nested fields, including automatically identifying and replacing raw binary data (like embedded images or large byte arrays) with a size placeholder (\<Binary Data: X bytes\>) to prevent console overflow and LLM token waste.  
* **Universal Compatibility:** Uses basic Pandas operations (df.head(1)) for broad compatibility across different Pandas/PyArrow versions.

## **🛠️ Requirements**

To run this script, you must have Python installed, along with the required packages listed in requirements.txt.

### **Installation**

pip install \-r requirements.txt

## **🚀 Usage**

1. **Run from your terminal:**  
   python scripts/parquet\_inspector.py \<path/to/your/file.parquet\>

2. **Output:** The script prints two main sections to the console:  
   * **Structured Metadata (JSON):** A full JSON object containing file stats, column names, pandas types, Python types, a sanitized sample value, and notes on complex data handling. This is ideal for copying directly into an LLM prompt.  
   * **Quick Summary:** A human-readable table summarizing the key column definitions.

### **Example Console Output (JSON Snippet)**

{  
    "file\_name": "sample.parquet",  
    "total\_rows": 1000000,  
    "total\_columns": 5,  
    "schema\_details": \[  
        {  
            "column\_name": "image\_data",  
            "pandas\_dtype": "object",  
            "sample\_value": {  
                "bytes": "\<Binary Data: 338392 bytes\>",  
                "path": null  
            },  
            "notes": "This column contains binary data."  
        },  
        {  
            "column\_name": "labels",  
            "pandas\_dtype": "object",  
            "sample\_value": "\<List, First Item Type: str, Total Items: 3\>",  
            "notes": "List structure. Sampled first element: example\_label\_1"  
        }  
    \]  
}  

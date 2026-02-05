def test_excel_reading():
    """Test Excel reading functionality"""
    print("Testing Excel reading...")
    
    folder = get_input_folder()
    files = list_excel_files_in_folder()
    
    if not files:
        print("No files found")
        return
    
    test_file = files[0]
    print(f"Testing with file: {test_file}")
    
    try:
        tables = read_excel_from_dataiku(test_file)
        print(f"Successfully read {len(tables)} sheets")
        
        for sheet_name, df in tables.items():
            print(f"  Sheet '{sheet_name}': {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            
    except Exception as e:
        print(f"Error: {e}")

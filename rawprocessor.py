# =============================================================================
# DATAIKU RAW EXTRACTION FILTER (SINGLE FILE VERSION)
# =============================================================================
# Reads ONE Excel file from a Dataiku folder
# Filters rows where:
#   1. Regular_Count == Consecutive_Count
#   2. Regular_Count > 2
# Writes filtered Excel back to Dataiku output folder
# =============================================================================

import dataiku
import pandas as pd
import io

# =============================================================================
# CONFIGURATION (UPDATE FOLDER IDs)
# =============================================================================
INPUT_FOLDER_ID = "xFGhJtYE"        # Input Dataiku folder (contains ONE Excel file)
OUTPUT_FOLDER_ID = "output_folder_id"  # Output Dataiku folder

# =============================================================================
# MAIN LOGIC
# =============================================================================

def main():
    print("=" * 60)
    print("DATAIKU RAW EXTRACTION FILTER - SINGLE FILE")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # 1. GET INPUT FOLDER AND FILE
    # -------------------------------------------------------------------------
    input_folder = dataiku.Folder(INPUT_FOLDER_ID)
    files = input_folder.list_paths_in_partition()

    if not files:
        raise Exception("❌ No file found in input folder")

    if len(files) > 1:
        raise Exception("❌ More than one file found. This script expects ONLY ONE file.")

    input_file = files[0]

    if not input_file.lower().endswith((".xlsx", ".xls")):
        raise Exception(f"❌ Input file is not an Excel file: {input_file}")

    print(f"📥 Input file detected: {input_file}")

    # -------------------------------------------------------------------------
    # 2. READ EXCEL FROM DATAIKU
    # -------------------------------------------------------------------------
    with input_folder.get_download_stream(input_file) as stream:
        excel_bytes = stream.read()

    df = pd.read_excel(io.BytesIO(excel_bytes))
    print(f"✅ Loaded Excel: {len(df)} rows | {len(df.columns)} columns")

    # -------------------------------------------------------------------------
    # 3. AUTO-DETECT REQUIRED COLUMNS
    # -------------------------------------------------------------------------
    regular_col = None
    consecutive_col = None

    for col in df.columns:
        col_lower = str(col).lower()
        if "regular" in col_lower and "count" in col_lower:
            regular_col = col
        elif "consecutive" in col_lower and "count" in col_lower:
            consecutive_col = col

    if not regular_col or not consecutive_col:
        raise Exception(
            f"❌ Required columns not found.\n"
            f"   Expected columns containing 'regular count' and 'consecutive count'\n"
            f"   Found columns: {list(df.columns)}"
        )

    print(f"🔍 Regular count column     : {regular_col}")
    print(f"🔍 Consecutive count column : {consecutive_col}")

    # -------------------------------------------------------------------------
    # 4. APPLY FILTERS
    # -------------------------------------------------------------------------
    df[regular_col] = pd.to_numeric(df[regular_col], errors="coerce")
    df[consecutive_col] = pd.to_numeric(df[consecutive_col], errors="coerce")

    mask = (df[regular_col] == df[consecutive_col]) & (df[regular_col] > 2)
    df_filtered = df[mask].copy()

    print(f"🎯 Rows after filtering: {len(df_filtered)}")

    if df_filtered.empty:
        print("⚠️ No rows matched the filter criteria. Output file will not be created.")
        return

    # -------------------------------------------------------------------------
    # 5. WRITE FILTERED EXCEL TO DATAIKU
    # -------------------------------------------------------------------------
    output_folder = dataiku.Folder(OUTPUT_FOLDER_ID)

    output_filename = input_file.rsplit(".", 1)[0] + "_filtered.xlsx"
    print(f"💾 Writing output file: {output_filename}")

    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
        df_filtered.to_excel(writer, index=False, sheet_name="Filtered_Data")

    with output_folder.get_writer(output_filename) as writer:
        writer.write(output_buffer.getvalue())

    print("✅ Successfully written filtered Excel to Dataiku")
    print("=" * 60)

# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":
    main()

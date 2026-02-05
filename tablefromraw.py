# =============================================================================
# DATAIKU TABLE GROUPING & STRUCTURE ANALYSIS
# =============================================================================
# Reads ONE Excel file from a Dataiku folder
# Groups consecutive rows into tables based on:
#   - Same Regular_Count and Consecutive_Count
# Writes grouped tables & analysis back to Dataiku
# =============================================================================

import dataiku
import pandas as pd
import io
import json
from typing import List, Dict

# =============================================================================
# CONFIGURATION
# =============================================================================
INPUT_FOLDER_ID = "xFGhJtYE"          # Input folder (ONE Excel file)
OUTPUT_FOLDER_ID = "output_folder_id" # Output folder

# =============================================================================
# CORE LOGIC
# =============================================================================

def group_into_tables(df: pd.DataFrame) -> List[pd.DataFrame]:
    if df.empty:
        return []

    regular_col = None
    consecutive_col = None

    for col in df.columns:
        col_l = col.lower()
        if "regular" in col_l and "count" in col_l:
            regular_col = col
        elif "consecutive" in col_l and "count" in col_l:
            consecutive_col = col

    if not regular_col or not consecutive_col:
        raise Exception("Required count columns not found")

    df = df.reset_index(drop=True)
    tables = []
    current_table = []
    current_counts = None

    for _, row in df.iterrows():
        pair = (row[regular_col], row[consecutive_col])

        if current_counts is None or pair == current_counts:
            current_counts = pair
            current_table.append(row)
        else:
            tables.append(pd.DataFrame(current_table).reset_index(drop=True))
            current_table = [row]
            current_counts = pair

    if current_table:
        tables.append(pd.DataFrame(current_table).reset_index(drop=True))

    return tables


def extract_table_structure(table_df: pd.DataFrame, table_num: int) -> Dict:
    structure = {
        "table_number": table_num,
        "row_count": len(table_df),
        "columns": list(table_df.columns),
        "column_count": len(table_df.columns),
    }

    for col in table_df.columns:
        col_l = col.lower()
        if "regular" in col_l and "count" in col_l:
            structure["regular_count"] = table_df[col].iloc[0]
        elif "consecutive" in col_l and "count" in col_l:
            structure["consecutive_count"] = table_df[col].iloc[0]

    if "regular_count" in structure and "consecutive_count" in structure:
        structure["counts_match"] = (
            structure["regular_count"] == structure["consecutive_count"]
        )

    if "Section" in table_df.columns:
        sections = table_df["Section"].dropna().unique().tolist()
        structure["sections"] = sections
        structure["section_count"] = len(sections)

    if "Page" in table_df.columns:
        pages = table_df["Page"].unique().tolist()
        structure["pages"] = pages
        structure["page_count"] = len(pages)

    if "Label" in table_df.columns:
        structure["sample_labels"] = table_df["Label"].head(5).tolist()

    return structure


def save_tables_to_dataiku(
    tables: List[pd.DataFrame], output_filename: str
) -> None:
    output_buffer = io.BytesIO()

    with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
        # ---------------------------
        # All tables combined
        combined = []
        for i, table in enumerate(tables, 1):
            t = table.copy()
            t.insert(0, "Table_Number", i)
            combined.append(t)

        combined_df = pd.concat(combined, ignore_index=True)
        combined_df.to_excel(writer, sheet_name="All_Tables", index=False)

        # ---------------------------
        # Individual table sheets
        for i, table in enumerate(tables, 1):
            sheet_name = f"Table_{i}"[:31]
            t = table.copy()
            t.insert(0, "Row_In_Table", range(1, len(t) + 1))
            t.to_excel(writer, sheet_name=sheet_name, index=False)

        # ---------------------------
        # Table statistics
        stats = []
        structures = []

        for i, table in enumerate(tables, 1):
            s = extract_table_structure(table, i)
            structures.append(s)

            stats.append({
                "Table_Number": i,
                "Row_Count": s.get("row_count"),
                "Regular_Count": s.get("regular_count"),
                "Consecutive_Count": s.get("consecutive_count"),
                "Counts_Match": s.get("counts_match"),
                "Section_Count": s.get("section_count", 0),
                "Page_Count": s.get("page_count", 0),
                "Sections": ", ".join(map(str, s.get("sections", [])))[:100],
                "Sample_Labels": ", ".join(map(str, s.get("sample_labels", [])))[:100],
            })

        pd.DataFrame(stats).to_excel(
            writer, sheet_name="Table_Statistics", index=False
        )

        json_df = pd.DataFrame(
            {"Table_Structures": [json.dumps(structures, indent=2)]}
        )
        json_df.to_excel(writer, sheet_name="Table_Structures", index=False)

    output_folder = dataiku.Folder(OUTPUT_FOLDER_ID)
    with output_folder.get_writer(output_filename) as w:
        w.write(output_buffer.getvalue())


# =============================================================================
# MAIN (DATAIKU ENTRY POINT)
# =============================================================================

def main():
    input_folder = dataiku.Folder(INPUT_FOLDER_ID)
    files = input_folder.list_paths_in_partition()

    if len(files) != 1:
        raise Exception("Input folder must contain exactly ONE Excel file")

    input_file = files[0]

    with input_folder.get_download_stream(input_file) as stream:
        df = pd.read_excel(io.BytesIO(stream.read()), engine="openpyxl")

    tables = group_into_tables(df)

    if not tables:
        raise Exception("No tables were formed")

    output_file = input_file.rsplit(".", 1)[0] + "_grouped.xlsx"
    save_tables_to_dataiku(tables, output_file)

    print(f"✅ Successfully grouped {len(tables)} tables")
    print(f"📁 Output written to Dataiku folder: {output_file}")


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":
    main()

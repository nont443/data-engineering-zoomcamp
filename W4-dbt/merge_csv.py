from pathlib import Path
import shutil

# Folder containing CSVs
in_dir = Path("./fhv")          # e.g. Path("./data")
out_file = Path("fhv_tripdata.csv")

csv_files = sorted(in_dir.glob("*.csv"))  # or provide a list manually
assert csv_files, "No CSV files found"

with out_file.open("wb") as w:
    # write header + all rows from first file
    with csv_files[0].open("rb") as f:
        shutil.copyfileobj(f, w)

    # append rows from remaining files (skip header line)
    for p in csv_files[1:]:
        with p.open("rb") as f:
            _ = f.readline()  # skip header
            shutil.copyfileobj(f, w)

print(f"✅ Wrote {out_file} from {len(csv_files)} files")

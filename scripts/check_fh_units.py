import csv
import sys
from pathlib import Path


def usage():
    print("Usage: python scripts/check_fh_units.py <path-to-consolidated-csv>")
    sys.exit(1)


def main():
    if len(sys.argv) != 2:
        usage()

    csv_path = Path(sys.argv[1]).expanduser().resolve()
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        sys.exit(2)

    total_rows = 0
    mileage_rows = 0
    missing_units = 0
    fallback_to_15 = 0
    samples = []

    with csv_path.open(newline='', encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            total_rows += 1
            hcpcs = (row.get("HCPCS") or "").strip().upper()
            if hcpcs not in {"A0425", "A0435", "A0436"}:
                continue
            mileage_rows += 1

            service_units = (row.get("SERVICE UNITS") or "").strip()
            original_units = (row.get("SVC_OriginalUnits_L2110_SVC") or "").strip()
            fh_units_used = (row.get("FAIR HEALTH UNITS USED") or "").strip()

            no_units = (not service_units or service_units == "0") and (not original_units or original_units == "0")
            if no_units:
                missing_units += 1
                if fh_units_used == "15":
                    fallback_to_15 += 1
                if len(samples) < 5:
                    samples.append(
                        {
                            "PAYOR": row.get("PAYOR PAID", ""),
                            "RUN": row.get("RUN", ""),
                            "FH Units": fh_units_used,
                            "Out Final": row.get("OUT OF NETWORK FINAL", ""),
                            "In Final": row.get("IN NETWORK FINAL", ""),
                        }
                    )

    print(f"File: {csv_path}")
    print(f"Total rows: {total_rows}")
    print(f"Mileage rows: {mileage_rows}")
    print(f"Rows with missing units: {missing_units}")
    print(f"Missing units showing 15: {fallback_to_15}")
    if samples:
        print("\nSample rows with missing units:")
        for sample in samples:
            print(sample)


if __name__ == "__main__":
    main()


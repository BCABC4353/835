"""
Fair Health Rates Parser

Parses rate data from Fair Health Excel files and provides lookup functionality
for rates based on ZIP code, HCPCS code, and optional service date.

Column mapping from source Excel:
- "Enter the location where you will be receiving or have received medical care" = ZIP Code
- "Date (GMT)" = date
- "Enter a Procedure Code or Keyword" = HCPCS
- "Out of Network" = Rate 1
- "In-Network" = Rate 2
"""

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import openpyxl
except ImportError:
    openpyxl = None


def normalize_hcpcs(hcpcs: Any) -> Optional[str]:
    """
    Normalize HCPCS code: uppercase, strip whitespace, remove non-alphanumeric.
    Returns None if input is empty or invalid.
    """
    if hcpcs is None:
        return None
    s = str(hcpcs).strip().upper()
    if not s or s.lower() in ('undefined', 'null', 'none', 'n/a', ''):
        return None
    # Remove non-alphanumeric characters
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s if s else None


def normalize_zip(zip_code: Any) -> Optional[int]:
    """
    Normalize ZIP code to 5-digit integer.
    Handles ZIP+4 format (12345-6789) and string inputs.
    Returns None if invalid.
    """
    if zip_code is None:
        return None
    s = str(zip_code).strip()
    if not s or s.lower() in ('undefined', 'null', 'none', 'n/a', ''):
        return None
    # Handle ZIP+4 format
    if '-' in s:
        s = s.split('-')[0]
    # Extract first 5 digits
    digits = re.sub(r'[^0-9]', '', s)
    if len(digits) >= 5:
        return int(digits[:5])
    elif digits:
        return int(digits)
    return None


def normalize_rate(rate: Any) -> Union[int, float, None]:
    """
    Normalize rate value.
    Returns numeric value or None (filters out N/A and non-numeric values).
    """
    if rate is None:
        return None
    s = str(rate).strip()
    if not s:
        return None
    # Remove currency symbols first, then check for undefined
    cleaned = re.sub(r'[$,]', '', s).strip()
    # Filter out all non-numeric placeholder values
    if not cleaned or cleaned.lower() in ('undefined', 'null', 'none', '', 'n/a', 'error'):
        return None
    # Try to parse as number
    try:
        if '.' in cleaned:
            return float(cleaned)
        return int(cleaned)
    except ValueError:
        # Non-numeric value - return None
        return None


def format_date(value: Any) -> str:
    """
    Format date as MM/DD/YY with leading zeros.
    """
    if value is None or value == '':
        return ''
    
    if isinstance(value, datetime):
        return value.strftime('%m/%d/%y')
    if isinstance(value, date):
        return value.strftime('%m/%d/%y')
    
    s_value = str(value).strip()
    
    # Try common date formats
    date_formats = [
        ('%Y%m%d', r'^\d{8}$'),
        ('%Y-%m-%d', r'^\d{4}-\d{2}-\d{2}$'),
        ('%m/%d/%Y', r'^\d{1,2}/\d{1,2}/\d{4}$'),
        ('%m/%d/%y', r'^\d{1,2}/\d{1,2}/\d{2}$'),
    ]
    
    for fmt, pattern in date_formats:
        if re.match(pattern, s_value):
            try:
                dt_obj = datetime.strptime(s_value, fmt)
                return dt_obj.strftime('%m/%d/%y')
            except ValueError:
                continue
    
    # Try ISO format with time
    try:
        dt_obj = datetime.fromisoformat(s_value.replace('Z', '+00:00'))
        return dt_obj.strftime('%m/%d/%y')
    except ValueError:
        pass
    
    return s_value


class FairHealthRates:
    """
    Manages Fair Health rate data with date-aware lookups.
    
    Stores rates in two structures:
    - rate_ranges: Historical rates with date ranges for service date lookups
    - current_rates: Most recent rates for quick lookups
    """
    
    def __init__(self):
        self.rate_ranges: Dict[Tuple[int, str], List[dict]] = {}
        self.current_rates: Dict[Tuple[int, str], dict] = {}
        self.load_stats = {
            'rows_processed': 0,
            'rows_skipped': 0,
            'unique_zips': set(),
            'unique_hcpcs': set()
        }
    
    def load_from_excel(self, filepath: str) -> dict:
        """
        Load rate data from Excel file.
        
        Returns dict with load statistics.
        """
        if openpyxl is None:
            raise ImportError("openpyxl is required to read Excel files. Install with: pip install openpyxl")
        
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        
        # Get headers from first row
        headers = []
        for cell in next(ws.iter_rows(min_row=1, max_row=1)):
            headers.append(str(cell.value).strip() if cell.value else '')
        
        # Map column names
        col_map = {}
        for idx, header in enumerate(headers):
            header_lower = header.lower()
            if 'location' in header_lower and 'medical care' in header_lower:
                col_map['zip'] = idx
            elif 'date' in header_lower and 'gmt' in header_lower:
                col_map['date'] = idx
            elif 'procedure code' in header_lower or 'keyword' in header_lower:
                col_map['hcpcs'] = idx
            elif 'out of network' in header_lower:
                col_map['rate1'] = idx
            elif 'in-network' in header_lower or 'in network' in header_lower:
                col_map['rate2'] = idx
        
        # Collect raw data grouped by (zip, hcpcs)
        raw_data: Dict[Tuple[int, str], List[dict]] = {}
        
        for row in ws.iter_rows(min_row=2):
            self.load_stats['rows_processed'] += 1
            
            values = [cell.value for cell in row]
            
            # Get values
            zip_val = values[col_map.get('zip', 0)] if 'zip' in col_map else None
            date_val = values[col_map.get('date', 1)] if 'date' in col_map else None
            hcpcs_val = values[col_map.get('hcpcs', 2)] if 'hcpcs' in col_map else None
            rate1_val = values[col_map.get('rate1', 3)] if 'rate1' in col_map else None
            rate2_val = values[col_map.get('rate2', 4)] if 'rate2' in col_map else None
            
            # Normalize values
            zip_code = normalize_zip(zip_val)
            hcpcs = normalize_hcpcs(hcpcs_val)
            rate1 = normalize_rate(rate1_val)
            rate2 = normalize_rate(rate2_val)
            
            # Skip invalid rows
            if zip_code is None or hcpcs is None:
                self.load_stats['rows_skipped'] += 1
                continue
            
            # Skip rows with no valid rates
            if rate1 is None and rate2 is None:
                self.load_stats['rows_skipped'] += 1
                continue
            
            # Parse date
            if isinstance(date_val, datetime):
                entry_date = date_val.date()
            elif isinstance(date_val, date):
                entry_date = date_val
            else:
                # Try to parse string date
                try:
                    if date_val:
                        s = str(date_val).strip()
                        # Try ISO format
                        entry_date = datetime.fromisoformat(s.replace('Z', '+00:00')).date()
                    else:
                        entry_date = date.today()
                except ValueError:
                    entry_date = date.today()
            
            key = (zip_code, hcpcs)
            if key not in raw_data:
                raw_data[key] = []
            
            raw_data[key].append({
                'date': entry_date,
                'rate1': rate1,
                'rate2': rate2
            })
            
            self.load_stats['unique_zips'].add(zip_code)
            self.load_stats['unique_hcpcs'].add(hcpcs)
        
        wb.close()
        
        # Build rate ranges from raw data
        self._build_rate_ranges(raw_data)
        
        return {
            'rows_processed': self.load_stats['rows_processed'],
            'rows_skipped': self.load_stats['rows_skipped'],
            'unique_zips': len(self.load_stats['unique_zips']),
            'unique_hcpcs': len(self.load_stats['unique_hcpcs']),
            'rate_keys': len(self.rate_ranges)
        }
    
    def _build_rate_ranges(self, raw_data: Dict[Tuple[int, str], List[dict]]):
        """
        Build consolidated date ranges from daily entries.
        
        Consecutive days with the same rates are consolidated into ranges.
        """
        for key, entries in raw_data.items():
            # Sort by date
            entries.sort(key=lambda x: x['date'])
            
            ranges = []
            current_range = None
            
            for entry in entries:
                if current_range is None:
                    # Start new range
                    current_range = {
                        'start_date': entry['date'],
                        'end_date': entry['date'],
                        'rate1': entry['rate1'],
                        'rate2': entry['rate2']
                    }
                elif (entry['rate1'] == current_range['rate1'] and 
                      entry['rate2'] == current_range['rate2'] and
                      entry['date'] <= current_range['end_date'] + timedelta(days=1)):
                    # Extend current range
                    current_range['end_date'] = entry['date']
                else:
                    # Save current range and start new one
                    ranges.append(current_range)
                    current_range = {
                        'start_date': entry['date'],
                        'end_date': entry['date'],
                        'rate1': entry['rate1'],
                        'rate2': entry['rate2']
                    }
            
            # Don't forget the last range
            if current_range:
                ranges.append(current_range)
            
            self.rate_ranges[key] = ranges
            
            # Set current rate as the most recent
            if ranges:
                latest = ranges[-1]
                self.current_rates[key] = {
                    'rate1': latest['rate1'],
                    'rate2': latest['rate2'],
                    'as_of': latest['end_date']
                }
    
    def get_zip_codes(self) -> List[int]:
        """
        Get all unique ZIP codes in the loaded rates data.
        
        Returns:
            Sorted list of ZIP codes as integers
        """
        return sorted(self.load_stats['unique_zips'])
    
    def get_rate(self, zip_code: int, hcpcs: str, service_date: Optional[date] = None) -> Optional[Tuple[Any, Any]]:
        """
        Look up rates for a given ZIP and HCPCS code.
        
        Args:
            zip_code: 5-digit ZIP code as integer
            hcpcs: HCPCS procedure code (will be normalized)
            service_date: Optional service date for historical lookup
        
        Returns:
            Tuple of (rate1, rate2) or None if not found
        """
        hcpcs_norm = normalize_hcpcs(hcpcs)
        if hcpcs_norm is None:
            return None
        
        key = (zip_code, hcpcs_norm)
        
        if service_date is None:
            # Return current rate
            if key in self.current_rates:
                cr = self.current_rates[key]
                return (cr['rate1'], cr['rate2'])
            return None
        
        # Look up by service date
        if key not in self.rate_ranges:
            return None
        
        for rate_range in self.rate_ranges[key]:
            if rate_range['start_date'] <= service_date <= rate_range['end_date']:
                return (rate_range['rate1'], rate_range['rate2'])
        
        # If no exact match, return the most recent rate before the service date
        applicable = [r for r in self.rate_ranges[key] if r['end_date'] <= service_date]
        if applicable:
            latest = max(applicable, key=lambda x: x['end_date'])
            return (latest['rate1'], latest['rate2'])
        
        return None
    
    def get_rates_for_all_zips(self, hcpcs: str, service_date: Optional[date] = None) -> Dict[int, Tuple[Any, Any]]:
        """
        Look up rates for a given HCPCS code across all ZIP codes.
        
        Args:
            hcpcs: HCPCS procedure code (will be normalized)
            service_date: Optional service date for historical lookup
        
        Returns:
            Dict mapping ZIP code to (out_of_network, in_network) tuple
        """
        results = {}
        for zip_code in self.get_zip_codes():
            rate = self.get_rate(zip_code, hcpcs, service_date)
            if rate:
                results[zip_code] = rate
        return results
    
    def summary(self) -> str:
        """
        Generate a summary of loaded rate data.
        """
        lines = [
            "Fair Health Rates Summary",
            "=" * 40,
            f"Unique ZIP codes: {len(self.load_stats['unique_zips'])}",
            f"Unique HCPCS codes: {len(self.load_stats['unique_hcpcs'])}",
            f"Rate combinations: {len(self.rate_ranges)}",
            f"Rows processed: {self.load_stats['rows_processed']}",
            f"Rows skipped: {self.load_stats['rows_skipped']}",
            "",
            "Sample rates (first 10):",
            "-" * 40
        ]
        
        for i, (key, ranges) in enumerate(list(self.rate_ranges.items())[:10]):
            zip_code, hcpcs = key
            current = self.current_rates.get(key, {})
            rate1 = current.get('rate1', '')
            rate2 = current.get('rate2', '')
            as_of = current.get('as_of')
            as_of_str = format_date(as_of) if as_of else ''
            
            lines.append(f"  ZIP {zip_code} / {hcpcs}: ${rate1} (OON), ${rate2} (IN) as of {as_of_str}")
            
            if len(ranges) > 1:
                lines.append(f"    ({len(ranges)} date ranges)")
                for r in ranges[:3]:
                    start_str = format_date(r['start_date'])
                    end_str = format_date(r['end_date'])
                    lines.append(f"      {start_str} - {end_str}: ${r['rate1']} / ${r['rate2']}")
                if len(ranges) > 3:
                    lines.append(f"      ... and {len(ranges) - 3} more ranges")
        
        return "\n".join(lines)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python rates.py <excel_file>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    rates = FairHealthRates()
    
    try:
        stats = rates.load_from_excel(filepath)
        print(f"Loaded {stats['rate_keys']} rate combinations")
        print(rates.summary())
    except Exception as e:
        print(f"Error loading rates: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


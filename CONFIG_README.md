# 835 EDI Parser Configuration Guide

## System Requirements

- **Python**: >= 3.8
- **tkinter**: Usually bundled with Python. On some Linux distributions, install separately:
  - Ubuntu/Debian: `sudo apt install python3-tk`
  - Fedora: `sudo dnf install python3-tkinter`
  - macOS (Homebrew): `brew install python-tk`

## Dependencies & Installation

Before running the parser, install the required Python dependencies:

```bash
pip install -r requirements.txt
```

Or install directly:

```bash
pip install openpyxl>=3.1.0
```

**Note:** `openpyxl` is required for reading Fair Health RATES.xlsx files. All other dependencies are part of the Python standard library.

---

## Overview

The 835 parser now supports flexible configuration through multiple methods:

1. **Configuration File** (JSON format)
2. **Environment Variables**
3. **Programmatic Defaults**

Configuration is loaded in priority order: **Environment Variables** > **Config File** > **Defaults**

---

## Quick Start

### Option 1: Configuration File (Recommended)

1. Copy the example config file:
   ```bash
   copy 835_config.example.json 835_config.json
   ```

2. Edit `835_config.json` with your paths:
   ```json
   {
     "trips_csv_path": "C:\\Data\\Trips.csv",
     "rates_xlsx_path": "C:\\Data\\RATES.xlsx"
   }
   ```

3. The parser will auto-discover `835_config.json` in:
   - Current working directory
   - User's home directory

### Option 2: Environment Variables

Set environment variables to override any setting:

**Windows (PowerShell):**
```powershell
$env:EDI_TRIPS_CSV_PATH = "C:\Data\Trips.csv"
$env:EDI_RATES_XLSX_PATH = "C:\Data\RATES.xlsx"
$env:EDI_LOG_LEVEL = "DEBUG"
```

**Windows (CMD):**
```cmd
set EDI_TRIPS_CSV_PATH=C:\Data\Trips.csv
set EDI_RATES_XLSX_PATH=C:\Data\RATES.xlsx
set EDI_LOG_LEVEL=DEBUG
```

**Linux/Mac:**
```bash
export EDI_TRIPS_CSV_PATH="/data/Trips.csv"
export EDI_RATES_XLSX_PATH="/data/RATES.xlsx"
export EDI_LOG_LEVEL="DEBUG"
```

### Option 3: Programmatic Configuration

```python
from config import get_config

# Get config and modify at runtime
config = get_config()
config.set('trips_csv_path', r'C:\Custom\Path\Trips.csv')
config.set('log_level', 'DEBUG')

# Or load from specific file
config = get_config(config_file='path/to/custom_config.json')
```

---

## Configuration Options

### Input File Paths

| Config Key | Environment Variable | Default | Description |
|------------|---------------------|---------|-------------|
| `trips_csv_path` | `EDI_TRIPS_CSV_PATH` | `~/Desktop/Trips.csv` | Path to Trips.csv for ZIP lookup |
| `rates_xlsx_path` | `EDI_RATES_XLSX_PATH` | `~/Desktop/RATES.xlsx` | Path to Fair Health RATES.xlsx |

### Output File Names

| Config Key | Environment Variable | Default | Description |
|------------|---------------------|---------|-------------|
| `output_csv_name` | `EDI_OUTPUT_CSV_NAME` | `835_consolidated_output.csv` | Main output CSV filename |
| `output_csv_compact_name` | `EDI_OUTPUT_CSV_COMPACT_NAME` | `835_consolidated_output_compact.csv` | Compact CSV filename |
| `validation_report_txt_name` | `EDI_VALIDATION_REPORT_TXT` | `835_validation_report.txt` | Validation report text filename |
| `validation_report_html_name` | `EDI_VALIDATION_REPORT_HTML` | `835_validation_report.html` | Validation report HTML filename |

**Note:** Output files are created in the same folder as the input EDI files. These settings only control the filename, not the directory.

### Processing Options

| Config Key | Environment Variable | Default | Description |
|------------|---------------------|---------|-------------|
| `enable_fair_health_rates` | `EDI_ENABLE_FAIR_HEALTH` | `true` | Load and use Fair Health rates |
| `enable_trips_lookup` | `EDI_ENABLE_TRIPS_LOOKUP` | `true` | Load and use Trips.csv for ZIP lookup |
| `enable_compact_csv` | `EDI_ENABLE_COMPACT_CSV` | `true` | Generate compact CSV (empty columns removed) |

**Note:** Validation is automatic and always runs - it cannot be disabled.

**Environment Variable Values:** Use `true`, `1`, `yes`, or `on` for true; anything else is false.

### Logging Configuration

| Config Key | Environment Variable | Default | Description |
|------------|---------------------|---------|-------------|
| `log_level` | `EDI_LOG_LEVEL` | `INFO` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `log_file` | `EDI_LOG_FILE` | `null` | Path to log file (null = console only) |

---

## Configuration File Format

### JSON Configuration File

Create `835_config.json` in your working directory or home directory:

```json
{
  "trips_csv_path": "C:\\Data\\Trips.csv",
  "rates_xlsx_path": "C:\\Data\\RATES.xlsx",

  "output_csv_name": "835_output.csv",
  "output_csv_compact_name": "835_output_compact.csv",

  "enable_fair_health_rates": true,
  "enable_trips_lookup": true,
  "enable_compact_csv": true,

  "log_level": "INFO",
  "log_file": "C:\\Logs\\835_parser.log"
}
```

**Path Formats:**
- Windows: Use double backslashes `\\` or forward slashes `/`
- Use `~` for home directory: `"~/Documents/Trips.csv"`
- Absolute paths: `"C:\\Data\\file.csv"` or `"/data/file.csv"`

---

## Configuration Priority

Settings are applied in this order (later overrides earlier):

1. **Default values** (built into code)
2. **Config file** (`835_config.json`)
3. **Environment variables** (highest priority)

### Example Priority Behavior

**Config file (`835_config.json`):**
```json
{
  "trips_csv_path": "C:\\Data\\Trips.csv",
  "log_level": "INFO"
}
```

**Environment:**
```bash
EDI_LOG_LEVEL=DEBUG
```

**Result:**
- `trips_csv_path` = `"C:\\Data\\Trips.csv"` (from config file)
- `log_level` = `"DEBUG"` (environment variable overrides)

---

## Usage Examples

### Example 1: Development Environment

**File: `835_config.json`**
```json
{
  "trips_csv_path": "C:\\Dev\\TestData\\Trips.csv",
  "rates_xlsx_path": "C:\\Dev\\TestData\\RATES.xlsx",
  "log_level": "DEBUG",
  "log_file": "C:\\Dev\\Logs\\835_debug.log"
}
```

### Example 2: Production Environment (Environment Variables)

```bash
# Production server configuration
export EDI_TRIPS_CSV_PATH="/opt/data/production/Trips.csv"
export EDI_RATES_XLSX_PATH="/opt/data/production/RATES.xlsx"
export EDI_LOG_LEVEL="INFO"
export EDI_LOG_FILE="/var/log/835_parser.log"
export EDI_ENABLE_FAIR_HEALTH="true"
```

### Example 3: Quick Test (Disable Features)

```bash
# Disable optional features for faster testing
export EDI_ENABLE_FAIR_HEALTH="false"
export EDI_ENABLE_TRIPS_LOOKUP="false"
export EDI_ENABLE_COMPACT_CSV="false"
# Note: Validation always runs automatically and cannot be disabled
```

### Example 4: Programmatic Override

```python
from config import get_config

# Load base config
config = get_config('custom_config.json')

# Runtime overrides
config.set('log_level', 'DEBUG')
config.set('enable_fair_health_rates', False)

# Use in your code
from parser_835 import process_folder

# Config is automatically used
result = process_folder('path/to/edi/files')
```

---

## Troubleshooting

### Config File Not Found

The parser looks for config files in:
1. Current working directory: `./835_config.json`
2. Home directory: `~/835_config.json`
3. Current directory: `./.835config`
4. Home directory: `~/.835config`

**Solution:** Place your config file in one of these locations or specify path explicitly:
```python
config = get_config(config_file='/path/to/config.json')
```

### Environment Variables Not Working

**Check variable names:**
- Must use exact names: `EDI_TRIPS_CSV_PATH`, `EDI_LOG_LEVEL`, etc.
- Case-sensitive on Linux/Mac

**Check boolean values:**
- Use: `true`, `1`, `yes`, or `on` (case-insensitive)
- Anything else is treated as `false`

### Paths Not Resolved

**Windows paths:**
```json
"trips_csv_path": "C:\\Users\\Name\\Desktop\\Trips.csv"
```
or
```json
"trips_csv_path": "C:/Users/Name/Desktop/Trips.csv"
```

**Home directory expansion:**
```json
"trips_csv_path": "~/Desktop/Trips.csv"
```
Expands to: `C:\Users\YourName\Desktop\Trips.csv` (Windows)

---

## Saving Configuration

Save current runtime configuration:

```python
from config import get_config

config = get_config()

# Modify settings
config.set('log_level', 'DEBUG')

# Save to file
config.save('my_config.json')

# Or save to loaded config file
config.save()  # Overwrites original file
```

---

## Migration from Hardcoded Paths

### Old Code (Hardcoded)
```python
TRIPS_CSV_PATH = r"C:\Users\Brendan Cameron\Desktop\Trips.csv"
```

### New Code (Configurable)
```python
from config import get_config
trips_path = get_config().trips_csv_path
```

**Backward Compatibility:**
- Old hardcoded constants are deprecated but still work
- Update to use `get_config()` for full configurability

---

## Best Practices

1. **Don't commit `835_config.json`** - Add to `.gitignore`
2. **Commit `835_config.example.json`** - Template for team
3. **Use environment variables** for deployment-specific settings
4. **Use config file** for shared team settings
5. **Document custom configs** - Add comments to your config files

---

## Support

For issues or questions:
1. Check this README
2. Review `835_config.example.json`
3. Check environment variable names (case-sensitive)
4. Enable debug logging: `config.set('log_level', 'DEBUG')`

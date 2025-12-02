# Known Limitations

This document describes known limitations of the 835 EDI Parser that users and administrators should be aware of.

---

## 1. Memory Scaling for Large Batches

### Issue
The parser loads all service lines from all files into memory simultaneously. For very large batches (hundreds of files with thousands of service lines each), memory usage can become excessive.

### Technical Details
- `process_folder()` materializes all service lines in the `all_rows` list
- This list is normalized in-place and then reused for validation
- A batch of 100 files with 5,000 service lines each = 500,000 rows in memory
- Each row is a Python dictionary with ~100 fields
- Estimated memory: 500,000 rows × 10 KB/row = ~5 GB RAM

### Symptoms
- Application slows down significantly with large batches
- System may report high memory usage
- In extreme cases, application may crash with `MemoryError`

### Workarounds

**Option 1: Process in Smaller Batches**
- Split large folders into batches of 50-100 files
- Process each batch separately
- Combine output CSV files afterward (Excel or command line)

**Option 2: Increase Available RAM**
- Close other applications before processing
- Use a machine with more RAM (16+ GB recommended for large batches)

**Option 3: Disable Optional Features**
- Edit `835_config.json`:
  ```json
  {
    "enable_fair_health_rates": false,
    "enable_trips_lookup": false,
    "enable_compact_csv": false
  }
  ```
- This reduces memory overhead by skipping enrichment steps

### Tested Limits
| Batch Size | Service Lines | Memory Usage | Status |
|------------|---------------|--------------|--------|
| 10 files | 5,000 total | ~500 MB | ✅ Works well |
| 50 files | 25,000 total | ~2 GB | ✅ Works |
| 100 files | 50,000 total | ~4 GB | ⚠ Slow but functional |
| 500 files | 250,000 total | ~20 GB | ❌ May crash |

### Future Improvements
To fully resolve this issue would require architectural changes:
1. **Streaming processing**: Process one file at a time, write to CSV incrementally
2. **Database backend**: Store rows in SQLite instead of memory
3. **Chunked validation**: Validate in batches rather than all at once

These changes are significant and would require extensive testing to ensure output remains identical.

---

## 2. Single-Threaded Processing

### Issue
File processing happens in a single background thread. Multi-core CPUs are not utilized.

### Impact
- Processing speed is limited to single-core performance
- On a 16-core machine, only ~6% of CPU capacity is used

### Workarounds
- None available. This is a design limitation.
- However, most time is spent in I/O (reading files) rather than CPU, so parallelization would have limited benefit

---

## 3. No Incremental Progress for Large Files

### Issue
When loading large RATES.xlsx files (millions of rows), the application appears frozen for 30+ seconds with no progress indicator.

### Impact
- Users may think the application crashed
- May force-quit during legitimate loading

### Workarounds
- Be patient when you see "Loading Fair Health rates..."
- Typical RATES.xlsx with 500,000 rows takes 20-30 seconds to load

### Planned Fix
- Add progress percentage for Excel loading (Medium priority)

---

## 4. Windows Only

### Issue
The application is only tested and supported on Windows 10/11.

### Impact
- Mac and Linux users cannot use the installer
- Python source code should work cross-platform but is untested

### Workarounds
- Mac/Linux users can run from source:
  ```bash
  pip install -r requirements.txt
  python gui.py
  ```

---

## 5. No Undo/Cancel During Processing

### Issue
Once file processing starts, there is no "Cancel" button. Users must wait for completion or close the entire application.

### Impact
- If wrong folder selected, must wait for processing to finish
- Closing window mid-processing will warn user but may result in incomplete output

### Workarounds
- Double-check folder selection before clicking "Select 835 Folder"
- If you must stop: Close the window and click "Yes" when warned about incomplete files

### Planned Fix
- Add "Cancel" button during processing (Medium priority)

---

## 6. No Auto-Update Mechanism

### Issue
Users must manually download and install new versions. No built-in update checker.

### Workarounds
- Check GitHub releases page periodically: https://github.com/[your-org]/835-parser/releases
- Subscribe to release notifications on GitHub

---

## 7. Validation Reports Can Be Very Large

### Issue
HTML validation reports for large batches can exceed 50 MB, causing browsers to hang when opened.

### Impact
- Validation report HTML may be slow to load or crash browser
- Report file takes significant disk space

### Workarounds
- Use the text report (`835_validation_report.txt`) instead of HTML
- Open HTML report in a text editor to view raw data
- Process smaller batches to reduce report size

---

## 8. Configuration Not Validated in Real-Time

### Issue
Settings dialog accepts any file path without checking if files exist or are valid until processing starts.

### Impact
- User saves incorrect paths
- Error not discovered until processing fails

### Workarounds
- After configuring paths, process a small test folder to verify settings work

### Planned Fix
- Real-time file validation in Settings dialog (Medium priority)

---

## 9. No Support for 837 or Other EDI Transaction Types

### Issue
Parser only supports 835 Healthcare Claim Payment/Advice transactions.

### Impact
- Cannot process 837 (claims), 270/271 (eligibility), 276/277 (claim status), etc.

### Workarounds
- None. Use a different tool for other transaction types.

---

## Reporting Issues

If you encounter limitations not listed here, please report them:
1. Note the batch size (number of files, total service lines)
2. Describe the symptom (crash, slowness, error message)
3. Include the error from console output (if any)
4. Contact: BCABC support

---

## Document Version

- Last Updated: 2025-12-02
- Software Version: 1.0.0

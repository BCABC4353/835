# Configuration Cleanup - Removed Unused Options

**Date:** 2025-12-02
**Version:** 1.0.0 → 1.0.1 (pending)

## Summary

Removed three configuration options that were documented but never actually used by the application. These "fake knobs" created false confidence and complicated troubleshooting.

---

## Removed Options

### 1. `chunk_size` ❌ REMOVED

**Was documented as:** "Progress feedback interval (rows)"
**Default value:** 10000
**Environment variable:** `EDI_CHUNK_SIZE`
**Reality:** Never referenced anywhere in the codebase. No progress feedback interval exists.

**Why removed:**
- Completely unused - zero references in code
- Gave users false impression they could tune performance
- Support confusion: "I set chunk_size to 5000 but nothing changed"

---

### 2. `validation_verbose` ❌ REMOVED

**Was documented as:** "Show detailed validation progress"
**Default value:** true
**Environment variable:** None
**Reality:** Never referenced. Validation output is fixed.

**Why removed:**
- Unused - validation always runs with same output level
- No code path checks this setting
- Users toggling it would see no effect

---

### 3. `simple_log_format` ❌ REMOVED

**Was documented as:** "Use simple format without timestamps (for GUI)"
**Default value:** false
**Environment variable:** None
**Reality:** Hardcoded to `True` in `parser_835.py:4734`

**Why removed:**
- Config option never read
- Actual value is hardcoded: `configure_logging(simple_format=True)`
- Log format is always simple (no timestamps) for GUI
- Users changing this config would see no effect

---

## Impact Analysis

### User Impact
**NONE** - Since these options never worked, removing them doesn't change behavior.

Users who have these in their `835_config.json` files:
- File will still load (unknown keys are ignored)
- No errors or warnings
- Behavior unchanged

### Migration Required
**NO** - Existing config files don't need to be updated.

---

## Files Changed

1. **config.py**
   - Removed from `DEFAULTS` dict
   - Removed from `env_mapping` dict
   - Removed type conversion logic
   - **Net change:** -13 lines

2. **835_config.example.json**
   - Removed all three options
   - **Net change:** -5 lines

3. **CONFIG_README.md**
   - Removed from configuration tables
   - Removed from examples
   - **Net change:** -13 lines

---

## Configuration Options That Remain

### Input Paths
- `trips_csv_path` - Path to Trips.csv for ZIP lookup
- `rates_xlsx_path` - Path to Fair Health RATES.xlsx

### Output Filenames
- `output_csv_name` - Main CSV filename
- `output_csv_compact_name` - Compact CSV filename
- `validation_report_txt_name` - Text report filename
- `validation_report_html_name` - HTML report filename

### Processing Toggles (These DO work)
- `enable_fair_health_rates` - Enable/disable Fair Health rate lookup
- `enable_trips_lookup` - Enable/disable Trips.csv ZIP lookup
- `enable_compact_csv` - Enable/disable compact CSV generation

### Logging (Partially working)
- `log_level` - ✅ Works: Set to DEBUG, INFO, WARNING, ERROR
- `log_file` - ✅ Works: Path to log file (with `~` expansion)

---

## Lessons Learned

### Don't Document Options That Don't Exist
Adding a config option to `DEFAULTS` doesn't make it work. Code must:
1. Read the option: `config.get("option_name")`
2. Use the value to change behavior
3. Test that changing it has an effect

### Code Review Checklist for Config Options
Before documenting a configuration option:
- [ ] grep codebase for references to the option
- [ ] Verify option is read: `config.get("name")` or `config.name`
- [ ] Verify option changes behavior (not just default value)
- [ ] Test with option enabled and disabled
- [ ] Document what behavior actually changes

---

## Future Config Additions

If adding new config options, follow this process:

1. **Implement first:**
   ```python
   if config.get("new_option", default_value):
       # Do something different
   ```

2. **Test it works:**
   - Set option to `true` - verify behavior changes
   - Set option to `false` - verify behavior changes back

3. **Then document:**
   - Add to `DEFAULTS` in config.py
   - Add to CONFIG_README.md with accurate description
   - Add to 835_config.example.json

**Never document an option before it works.**

---

## References

- Issue reported: 2025-12-02
- Grep search confirmed zero usage
- Removed in commit: [pending]

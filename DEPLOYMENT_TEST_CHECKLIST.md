# Deployment Test Checklist

## Critical Tests Before Release

### 1. Clean Build Test
- [ ] Delete `dist/` and `build/` directories
- [ ] Run: `pyinstaller build_installer.spec`
- [ ] Verify exe created: `dist\835-EDI-Parser.exe`
- [ ] Check exe size (should be ~50-100 MB)

### 2. Standalone Executable Test
- [ ] Copy `dist\835-EDI-Parser.exe` to a DIFFERENT machine (or VM)
- [ ] Machine should NOT have Python installed
- [ ] Double-click exe - does GUI launch?
- [ ] No console window appears (console=False works)?

### 3. First Run Experience
- [ ] Delete any existing config: `%APPDATA%\835-EDI-Parser\835_config.json`
- [ ] Launch program
- [ ] Should show "First-Time Setup" dialog
- [ ] Click "Yes" - settings dialog opens
- [ ] Click "No" - see tip message in console

### 4. Configuration Test
- [ ] Open Settings dialog
- [ ] Browse and select Trips.csv
- [ ] Browse and select RATES.xlsx
- [ ] Click Save
- [ ] Verify config saved to: `%APPDATA%\835-EDI-Parser\835_config.json`
- [ ] Open config file - paths should show as full paths (not ~)

### 5. File Processing Test
- [ ] Click "Select 835 Folder"
- [ ] Choose folder with real 835 files
- [ ] Verify processing starts
- [ ] No crashes during processing
- [ ] Output files created in same folder:
  - [ ] `835_consolidated_output.csv`
  - [ ] `835_consolidated_output_compact.csv`
  - [ ] `835_validation_report.txt`
  - [ ] `835_validation_report.html`

### 6. Path Edge Cases
- [ ] Test with folder path containing spaces: `C:\My Documents\835 Files\`
- [ ] Test with special characters: `C:\Users\Test&User\835\`
- [ ] Test with long paths (>100 characters)

### 7. Error Handling
- [ ] Select empty folder - should show "No 835 files found"
- [ ] Select folder with non-835 files - should skip invalid files
- [ ] Close program during processing - no crash
- [ ] Launch without Trips.csv configured - should work but skip feature
- [ ] Launch without RATES.xlsx configured - should work but skip feature

### 8. Installer Test
- [ ] Run InnoSetup on `installer.iss`
- [ ] Verify installer created: `installer_output\835-EDI-Parser-Setup-v1.0.0.exe`
- [ ] Copy installer to different machine
- [ ] Run installer WITHOUT admin rights - should work
- [ ] If prompted for admin, choose "Install for current user only"
- [ ] Verify Start Menu shortcut created
- [ ] Launch from Start Menu - program works
- [ ] Uninstall - clean removal, no leftover files in Program Files

### 9. Multi-User Test (Windows)
- [ ] Install on machine with multiple users
- [ ] User A: Configure paths, process files
- [ ] User B: Launch program - should show first-run setup (separate config)
- [ ] Each user has own config in their own AppData

### 10. Production Data Test
- [ ] Process real production 835 files (100+ files)
- [ ] Verify all output columns populated correctly
- [ ] Check validation report for accuracy
- [ ] Compare output with previous version (if applicable)

## Failure Criteria

If ANY of these fail, DO NOT release:
- [ ] Program crashes on launch
- [ ] Cannot select folders
- [ ] Cannot process valid 835 files
- [ ] Config not saved/loaded correctly
- [ ] Output files not created
- [ ] Installer requires admin when it shouldn't
- [ ] Program shows Python errors to user

## Sign-Off

Only when ALL tests pass:
- [ ] Tested by: ________________
- [ ] Date: ________________
- [ ] Ready for release: YES / NO

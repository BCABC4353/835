# Building the 835 EDI Parser Installer

This guide explains how to create a standalone Windows installer for the 835 EDI Parser that non-technical users can download and install.

## Overview

The build process creates:
1. **Standalone .exe** - Single executable with all Python dependencies bundled (PyInstaller)
2. **Windows Installer** - Professional installer package (InnoSetup) that installs the .exe

## Prerequisites

### 1. Install Python Dependencies

```bash
pip install pyinstaller openpyxl
```

### 2. Install InnoSetup (for creating the installer)

Download and install InnoSetup from: https://jrsoftware.org/isdl.php

Choose the standard installation (InnoSetup 6.x or later).

## Build Steps

### Step 1: Build the Standalone Executable

Open a command prompt in the project directory and run:

```bash
pyinstaller build_installer.spec
```

This will:
- Bundle all Python code and dependencies into a single .exe
- Include HCPCS data files
- Include documentation files
- Create the executable in `dist\835-EDI-Parser.exe`

**Expected output:**
```
Building Analysis...
Building PYZ...
Building EXE...
Building EXE from EXE-00.toc completed successfully.
```

**Output location:** `dist\835-EDI-Parser.exe` (approximately 20-30 MB)

### Step 2: Test the Standalone Executable

Before creating the installer, test the executable:

```bash
cd dist
835-EDI-Parser.exe
```

The GUI should launch. Verify:
- ✅ Settings dialog opens (Settings → Configure File Paths)
- ✅ File browser dialogs work
- ✅ Settings save correctly (check `%APPDATA%\835-EDI-Parser\835_config.json`)
- ✅ Application can process a test 835 file

### Step 3: Build the Windows Installer

1. Open **InnoSetup Compiler** (installed in Step 2 of Prerequisites)

2. Open the installer script:
   - File → Open → Browse to `installer.iss`

3. Compile the installer:
   - Build → Compile (or press Ctrl+F9)

**Expected output:**
```
Compiling [Setup]
Compiling [Languages]
Compiling [Files]
Compiling [Icons]
Compiling [Run]
Compiling [Code]

Successful compile (X.XX seconds)
Output: installer_output\835-EDI-Parser-Setup-v1.0.0.exe
```

**Output location:** `installer_output\835-EDI-Parser-Setup-v1.0.0.exe`

This is your final distributable installer! (~25-35 MB)

## Testing the Installer

### Test on a Clean System (Recommended)

Ideally, test on a computer that doesn't have Python installed:

1. Copy `installer_output\835-EDI-Parser-Setup-v1.0.0.exe` to the test machine
2. Double-click the installer
3. Follow the installation wizard
4. Verify the application installs and runs correctly

### Installation Options

During installation, users will see:

1. **Welcome screen** - Introduction
2. **License agreement** - Shows LICENSE.txt
3. **Installation location** - Default: `C:\Program Files\835 EDI Parser\`
4. **File paths configuration** (OPTIONAL) - Users can specify:
   - Trips.csv location
   - RATES.xlsx location

   Or skip and configure later via Settings
5. **Desktop icon** - Optional shortcut
6. **Installation progress**
7. **Completion** - Option to launch immediately

### What Gets Installed

**Program Files:**
- `C:\Program Files\835 EDI Parser\`
  - `835-EDI-Parser.exe` - Main application
  - `CONFIG_README.md` - Documentation
  - `835_config.example.json` - Example config
  - `hcpcs_*.zip` - HCPCS data files
  - `unins000.exe` - Uninstaller

**User Data:**
- `%APPDATA%\835-EDI-Parser\`
  - `835_config.json` - User settings (created on first save)

**Start Menu:**
- `835 EDI Parser` - Launch application
- `Configuration Guide` - Open documentation
- `Uninstall 835 EDI Parser`

**Desktop (optional):**
- `835 EDI Parser` shortcut

## Distribution

### Single-File Distribution

Distribute the installer to users:

**File:** `installer_output\835-EDI-Parser-Setup-v1.0.0.exe`

Users simply:
1. Download the installer
2. Double-click to run
3. Follow the wizard
4. Use the application

No Python installation required!

### Where to Host

Options for distributing the installer:
- Company file share
- Internal website/portal
- Email (if under size limits)
- Cloud storage (Dropbox, Google Drive, OneDrive)
- GitHub Releases (if repository is private/internal)

## Updating the Version

To release a new version:

1. Update version number in `pyproject.toml`:
   ```toml
   version = "1.1.0"
   ```

2. Update version in `installer.iss`:
   ```pascal
   #define MyAppVersion "1.1.0"
   ```

3. Rebuild:
   ```bash
   pyinstaller build_installer.spec
   ```

4. Recompile installer in InnoSetup

5. New installer will be named: `835-EDI-Parser-Setup-v1.1.0.exe`

## Troubleshooting

### Problem: "PyInstaller not found"

**Solution:**
```bash
pip install pyinstaller
```

### Problem: Missing openpyxl imports in built .exe

**Solution:** Already handled in `build_installer.spec` hidden imports. If you see errors, verify:
```python
hiddenimports=[
    'openpyxl',
    'openpyxl.cell._writer',
    'openpyxl.styles.stylesheet',
    ...
]
```

### Problem: HCPCS files not included

**Solution:** Check that `hcpcs_*.zip` files exist in the project directory before building.

The spec file includes:
```python
datas=[
    ('hcpcs_*.zip', '.'),
    ...
]
```

### Problem: InnoSetup compile errors about missing LICENSE.txt

**Solution:** Ensure `LICENSE.txt` exists in the project directory. It was created as part of this setup.

### Problem: Settings not saving after installation

**Solution:** This should be fixed. Settings now save to:
- `%APPDATA%\835-EDI-Parser\835_config.json`

This location is always writable (unlike Program Files).

Verify in config.py lines 228-245 (the `_get_default_config_path()` method).

### Problem: Executable too large

**Current size:** ~25-35 MB (expected)

This includes:
- Python interpreter
- All dependencies (openpyxl, tkinter, etc.)
- Application code
- HCPCS data

To reduce size:
- Remove unused HCPCS files
- Use UPX compression (already enabled in spec file)

### Problem: Antivirus flags the executable

**Cause:** PyInstaller executables sometimes trigger false positives

**Solutions:**
1. Sign the executable with a code signing certificate (best for production)
2. Add exception in antivirus
3. Submit to antivirus vendor as false positive
4. Use `--noupx` flag if UPX compression triggers detection

## Advanced Options

### Adding an Application Icon

1. Create or obtain a `.ico` file (256x256 recommended)
2. Save as `app_icon.ico` in project directory
3. Update `build_installer.spec`:
   ```python
   icon='app_icon.ico',
   ```
4. Update `installer.iss`:
   ```pascal
   SetupIconFile=app_icon.ico
   ```
5. Rebuild

### Customizing the Installer

Edit `installer.iss` to customize:
- Company name (line 9)
- Install location
- License text
- Installer appearance
- Registry keys
- File associations

### Creating a Silent Installer

Users can run silent installation:
```bash
835-EDI-Parser-Setup-v1.0.0.exe /SILENT
```

Or very silent (no UI at all):
```bash
835-EDI-Parser-Setup-v1.0.0.exe /VERYSILENT
```

## Summary

**Quick Build Commands:**

```bash
# 1. Build executable
pyinstaller build_installer.spec

# 2. Test it
dist\835-EDI-Parser.exe

# 3. Open InnoSetup and compile installer.iss

# 4. Distribute
installer_output\835-EDI-Parser-Setup-v1.0.0.exe
```

**Result:** Professional Windows installer that non-technical users can download and install with just a few clicks!

## Support

If users encounter issues:
1. Check they have Windows 10/11 (Windows 7+ should work but not tested)
2. Verify they have administrator privileges for installation
3. Check antivirus isn't blocking the installer
4. Ensure sufficient disk space (~100 MB for installation + processing data)

Settings are stored in `%APPDATA%\835-EDI-Parser\835_config.json` - users can delete this to reset configuration.

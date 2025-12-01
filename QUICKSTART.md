# Quick Start: Creating Your Installer

This is a simplified guide to build your standalone Windows installer in 3 steps.

## Prerequisites (One-Time Setup)

1. **Install PyInstaller:**
   ```bash
   pip install pyinstaller
   ```

2. **Install InnoSetup:**
   - Download from: https://jrsoftware.org/isdl.php
   - Run the installer (takes 2 minutes)

## Build Your Installer (3 Steps)

### Step 1: Build the .exe
Open command prompt in this folder and run:
```bash
pyinstaller build_installer.spec
```

Wait for it to finish (~30-60 seconds). You'll see: `Building EXE from EXE-00.toc completed successfully.`

**Output:** `dist\835-EDI-Parser.exe`

### Step 2: Test the .exe (Optional but Recommended)
```bash
dist\835-EDI-Parser.exe
```

The GUI should open. Click around to verify it works.

### Step 3: Create the Installer
1. Open **InnoSetup Compiler** (from Start Menu)
2. File → Open → Select `installer.iss`
3. Build → Compile (or press Ctrl+F9)

Wait ~10 seconds. You'll see: `Successful compile`

**Output:** `installer_output\835-EDI-Parser-Setup-v1.0.0.exe`

## Done! 🎉

Your installer is ready: `installer_output\835-EDI-Parser-Setup-v1.0.0.exe`

Send this file to users. They just:
1. Download it
2. Double-click
3. Follow the wizard
4. Use the program

No Python needed on their computer!

## What Changed vs Development Version

**Settings Location:**
- ✅ OLD: Saved to application directory (not writable in Program Files)
- ✅ NEW: Saves to `%APPDATA%\835-EDI-Parser\835_config.json` (always writable)

**User Experience:**
- Can configure Trips.csv and RATES.xlsx paths during installation
- Or configure later via Settings menu
- All settings persist between sessions
- Clean uninstall available

## Next Steps

See `BUILD_INSTALLER.md` for:
- Detailed explanations
- Troubleshooting
- Version updates
- Advanced customization
- Distribution options

## Quick Rebuild

After making code changes:
```bash
pyinstaller build_installer.spec
# Then recompile installer.iss in InnoSetup
```

That's it!

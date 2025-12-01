; 835 EDI Parser - InnoSetup Installer Script
; This creates a Windows installer (.exe) for the 835 EDI Parser application
;
; Prerequisites:
; 1. Build the executable first with: pyinstaller build_installer.spec
; 2. Install InnoSetup from: https://jrsoftware.org/isdl.php
; 3. Compile this script with InnoSetup Compiler

#define MyAppName "835 EDI Parser"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Your Organization Name"
#define MyAppExeName "835-EDI-Parser.exe"

[Setup]
; Application information
AppId={{835-EDI-PARSER-2024}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
LicenseFile=LICENSE.txt
InfoBeforeFile=CONFIG_README.md
OutputDir=installer_output
OutputBaseFilename=835-EDI-Parser-Setup-v{#MyAppVersion}
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin

; UI settings
DisableProgramGroupPage=yes
DisableWelcomePage=no

; Uninstall settings
UninstallDisplayIcon={app}\{#MyAppExeName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; Main executable (built by PyInstaller)
Source: "dist\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion

; Documentation
Source: "CONFIG_README.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "835_config.example.json"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Start Menu shortcut
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Configuration Guide"; Filename: "{app}\CONFIG_README.md"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

; Desktop icon (optional)
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
; Option to run application after installation
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[Code]
// Custom page for configuring file paths at install time (optional)
var
  ConfigPage: TInputFileWizardPage;
  TripsPathEdit: String;
  RatesPathEdit: String;

procedure InitializeWizard;
begin
  // Create custom page for file path configuration
  ConfigPage := CreateInputFilePage(wpSelectDir,
    'Configure Data File Paths (Optional)',
    'You can set these paths now or configure them later in the Settings dialog.',
    'If you have the Trips.csv and RATES.xlsx files ready, you can specify their paths here. Otherwise, click Next and configure them later using Settings > Configure File Paths.');

  ConfigPage.Add('Location of Trips.csv file:',
    'CSV files (*.csv)|*.csv|All files (*.*)|*.*',
    '.csv');

  ConfigPage.Add('Location of RATES.xlsx file:',
    'Excel files (*.xlsx)|*.xlsx|All files (*.*)|*.*',
    '.xlsx');

  // Set prompts
  ConfigPage.Edits[0].Text := '';
  ConfigPage.Edits[1].Text := '';
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ConfigFilePath: String;
  ConfigContent: String;
  TripsPath: String;
  RatesPath: String;
begin
  if CurStep = ssPostInstall then
  begin
    // Get the paths from the custom page
    TripsPath := ConfigPage.Values[0];
    RatesPath := ConfigPage.Values[1];

    // Only create config file if user provided at least one path
    if (TripsPath <> '') or (RatesPath <> '') then
    begin
      // Create config file in user's AppData
      ConfigFilePath := ExpandConstant('{userappdata}\835-EDI-Parser\835_config.json');

      // Create directory if it doesn't exist
      ForceDirectories(ExtractFilePath(ConfigFilePath));

      // Build JSON config content
      ConfigContent := '{' + #13#10;

      if TripsPath <> '' then
      begin
        // Escape backslashes for JSON
        TripsPath := StringChangeEx(TripsPath, '\', '\\', [rfReplaceAll]);
        ConfigContent := ConfigContent + '  "trips_csv_path": "' + TripsPath + '"';
        if RatesPath <> '' then
          ConfigContent := ConfigContent + ',' + #13#10;
      end;

      if RatesPath <> '' then
      begin
        // Escape backslashes for JSON
        RatesPath := StringChangeEx(RatesPath, '\', '\\', [rfReplaceAll]);
        if TripsPath = '' then
          ConfigContent := ConfigContent + '  ';
        ConfigContent := ConfigContent + '"rates_xlsx_path": "' + RatesPath + '"' + #13#10;
      end;

      ConfigContent := ConfigContent + '}' + #13#10;

      // Save config file
      SaveStringToFile(ConfigFilePath, ConfigContent, False);
    end;
  end;
end;

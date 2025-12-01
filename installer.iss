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
SetupIconFile=app_icon.ico
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

; Documentation (optional - will skip if missing)
Source: "CONFIG_README.md"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "835_config.example.json"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "LICENSE.txt"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist

[Icons]
; Start Menu shortcut
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

; Desktop icon (optional)
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
; Option to run application after installation
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

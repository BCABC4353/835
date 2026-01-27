"""
835 EDI Parser - GUI Module

This module contains the graphical user interface for the 835 parser:
- ConsoleRedirector: Redirects console output to GUI text widget
- SettingsDialog: Configuration dialog for file paths
- ProcessingWindow: Main GUI window for processing 835 files
"""

import os
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import TclError, filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText


class ConsoleRedirector:
    """Redirects console output to a text widget (thread-safe)"""

    def __init__(self, text_widget, progress_text, root, operation_label=None):
        self.text_widget = text_widget
        self.progress_text = progress_text
        self.root = root
        self.operation_label = operation_label
        self.buffer = ""

    def write(self, text):
        """Write text to the widget (thread-safe using root.after)"""
        # Use root.after to safely update GUI from background thread
        self.root.after(0, lambda: self._write_to_widget(text))

        # Update operation label if text contains validation progress markers
        if self.operation_label and "[" in text and "]" in text:
            # Extract operation from text like "[1/9] Parsing EDI structure..."
            try:
                if text.strip().startswith("["):
                    operation = text.strip()
                    self.root.after(0, lambda op=operation: self.operation_label.config(text=f"⏳ {op}"))
            except (AttributeError, TclError):
                pass

    def _write_to_widget(self, text):
        """Internal method to write to widget on main thread"""
        self.text_widget.insert(tk.END, text)
        self.text_widget.see(tk.END)  # Auto-scroll to bottom
        self.text_widget.update_idletasks()

    def flush(self):
        """Flush buffer (required for file-like object)"""
        pass


class SettingsDialog:
    """Settings dialog for configuring file paths"""

    def __init__(self, parent, config):
        self.result = False
        self.config = config

        # Create dialog window
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Settings")
        self.dialog.geometry("600x480")
        self.dialog.resizable(False, False)
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Center on parent
        self.dialog.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() - 600) // 2
        y = parent.winfo_y() + (parent.winfo_height() - 480) // 2
        self.dialog.geometry(f"+{x}+{y}")

        self._create_widgets()

    def _create_widgets(self):
        """Create dialog widgets"""
        # Main frame with padding
        main_frame = ttk.Frame(self.dialog, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Title
        title_label = ttk.Label(main_frame, text="Configure File Paths", font=("Segoe UI", 12, "bold"))
        title_label.pack(pady=(0, 15))

        # Description
        desc_label = ttk.Label(
            main_frame,
            text="Configure file paths and output location.\n"
            "All settings are optional - leave blank to use defaults.",
            font=("Segoe UI", 9),
            foreground="#666666",
        )
        desc_label.pack(pady=(0, 15))

        # Fair Health ZIP CSV path (provides RUN -> ZIP mapping and patient payments)
        trips_frame = ttk.Frame(main_frame)
        trips_frame.pack(fill=tk.X, pady=5)

        ttk.Label(trips_frame, text="ZIP/Payments:", width=12).pack(side=tk.LEFT)
        self.trips_var = tk.StringVar(value=self.config.get("trips_csv_path") or "")
        trips_entry = ttk.Entry(trips_frame, textvariable=self.trips_var, width=50)
        trips_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(
            trips_frame,
            text="Browse...",
            command=lambda: self._browse_file(self.trips_var, "Fair Health ZIP CSV", [("CSV files", "*.csv")]),
        ).pack(side=tk.LEFT)

        # RATES path (Excel file, .gsheet file, or Google Sheet URL)
        rates_frame = ttk.Frame(main_frame)
        rates_frame.pack(fill=tk.X, pady=5)

        ttk.Label(rates_frame, text="RATES:", width=12).pack(side=tk.LEFT)
        self.rates_var = tk.StringVar(value=self.config.get("rates_xlsx_path") or "")
        rates_entry = ttk.Entry(rates_frame, textvariable=self.rates_var, width=50)
        rates_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(
            rates_frame,
            text="Browse...",
            command=lambda: self._browse_file(
                self.rates_var,
                "RATES File",
                [("Supported files", "*.xlsx *.gsheet"), ("Excel files", "*.xlsx"), ("Google Sheet links", "*.gsheet")],
            ),
        ).pack(side=tk.LEFT)

        # Add hint for Google Sheet support
        rates_hint_frame = ttk.Frame(main_frame)
        rates_hint_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(rates_hint_frame, text="", width=12).pack(side=tk.LEFT)
        ttk.Label(
            rates_hint_frame,
            text="(.xlsx, .gsheet from Google Drive, or Google Sheet URL)",
            font=("Segoe UI", 8),
            foreground="#666666",
        ).pack(side=tk.LEFT, padx=5)

        # Google Sheet Tab ID (gid) - for .gsheet files that don't specify which tab
        gid_frame = ttk.Frame(main_frame)
        gid_frame.pack(fill=tk.X, pady=5)

        ttk.Label(gid_frame, text="Sheet Tab:", width=12).pack(side=tk.LEFT)
        self.gid_var = tk.StringVar(value=self.config.get("rates_gid") or "")
        gid_entry = ttk.Entry(gid_frame, textvariable=self.gid_var, width=20)
        gid_entry.pack(side=tk.LEFT, padx=5)
        ttk.Label(
            gid_frame,
            text="(For .gsheet files: copy gid number from browser URL, e.g. 1510374061)",
            font=("Segoe UI", 8),
            foreground="#666666",
        ).pack(side=tk.LEFT, padx=5)

        # CSV Output folder path
        output_frame = ttk.Frame(main_frame)
        output_frame.pack(fill=tk.X, pady=5)

        ttk.Label(output_frame, text="CSV Output:", width=12).pack(side=tk.LEFT)
        self.output_var = tk.StringVar(value=self.config.get("output_folder") or "")
        output_entry = ttk.Entry(output_frame, textvariable=self.output_var, width=50)
        output_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(
            output_frame,
            text="Browse...",
            command=lambda: self._browse_directory(self.output_var, "CSV Output Folder"),
        ).pack(side=tk.LEFT)

        # Database folder path
        db_frame = ttk.Frame(main_frame)
        db_frame.pack(fill=tk.X, pady=5)

        ttk.Label(db_frame, text="Database:", width=12).pack(side=tk.LEFT)
        # Show directory portion of database path (without filename)
        db_config_path = self.config.get("database_path") or ""
        if db_config_path and db_config_path.endswith(".db"):
            db_config_path = os.path.dirname(db_config_path)
        self.db_var = tk.StringVar(value=db_config_path)
        db_entry = ttk.Entry(db_frame, textvariable=self.db_var, width=50)
        db_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(
            db_frame,
            text="Browse...",
            command=lambda: self._browse_directory(self.db_var, "Database Folder"),
        ).pack(side=tk.LEFT)

        # Deductible report output folder
        deduct_frame = ttk.Frame(main_frame)
        deduct_frame.pack(fill=tk.X, pady=5)

        ttk.Label(deduct_frame, text="Deduct Rpts:", width=12).pack(side=tk.LEFT)
        self.deduct_var = tk.StringVar(value=self.config.get("deductible_report_output_dir") or "")
        deduct_entry = ttk.Entry(deduct_frame, textvariable=self.deduct_var, width=50)
        deduct_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(
            deduct_frame,
            text="Browse...",
            command=lambda: self._browse_directory(self.deduct_var, "Deductible Report Output Folder"),
        ).pack(side=tk.LEFT)

        # Info about config file
        config_path = self.config._get_default_config_path()
        info_label = ttk.Label(
            main_frame, text=f"Settings saved to: {config_path}", font=("Segoe UI", 8), foreground="#999999"
        )
        info_label.pack(pady=(20, 10))

        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=10)

        ttk.Button(button_frame, text="OK", command=self._save, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self._cancel, width=10).pack(side=tk.LEFT, padx=5)

    def _browse_file(self, var, title, filetypes):
        """Open file browser dialog"""
        initial_dir = os.path.dirname(var.get()) if var.get() else os.path.expanduser("~")
        filepath = filedialog.askopenfilename(
            title=f"Select {title}", initialdir=initial_dir, filetypes=filetypes + [("All files", "*.*")]
        )
        if filepath:
            var.set(filepath)

    def _browse_directory(self, var, title):
        """Open directory browser dialog"""
        initial_dir = var.get() if var.get() else os.path.expanduser("~")
        dirpath = filedialog.askdirectory(title=f"Select {title}", initialdir=initial_dir)
        if dirpath:
            var.set(dirpath)

    def _save(self):
        """Save settings and close dialog"""
        # Update config
        trips_path = self.trips_var.get().strip()
        rates_path = self.rates_var.get().strip()
        rates_gid = self.gid_var.get().strip()
        output_path = self.output_var.get().strip()
        db_path = self.db_var.get().strip()
        deduct_path = self.deduct_var.get().strip()

        # If db_path doesn't end with .db, treat it as a directory and append the filename
        if db_path and not db_path.endswith(".db"):
            db_path = os.path.join(db_path, "edi_transactions.db")

        self.config.set("trips_csv_path", trips_path if trips_path else None)
        self.config.set("rates_xlsx_path", rates_path if rates_path else None)
        self.config.set("rates_gid", rates_gid if rates_gid else None)
        self.config.set("output_folder", output_path if output_path else None)
        self.config.set("database_path", db_path if db_path else None)
        self.config.set("deductible_report_output_dir", deduct_path if deduct_path else None)

        # Save to AppData location (same place config.py checks first on startup)
        try:
            config_path = self.config._get_default_config_path()
            self.config.save(str(config_path))
            self.result = True
            self.dialog.destroy()
        except Exception as e:
            messagebox.showerror("Error", f"Could not save config to {config_path}.\n\nError: {type(e).__name__}: {e}")

    def _cancel(self):
        """Close dialog without saving"""
        self.dialog.destroy()

    def show(self):
        """Show dialog and wait for it to close"""
        self.dialog.wait_window()
        return self.result


def check_first_run_config():
    """
    Check if this is a first run with no configuration.
    Returns (config, needs_setup) tuple.
    """
    from config import get_config

    config = get_config()

    # Check if both paths are None/empty (first run scenario)
    trips_path = config.get("trips_csv_path")
    rates_path = config.get("rates_xlsx_path")

    needs_setup = (not trips_path) and (not rates_path)
    return config, needs_setup


class ProcessingWindow:
    """Main GUI window for processing 835 files"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("835 File Processor")
        self.root.geometry("1000x700")
        self.root.configure(background="#f0f0f0")

        # Set application icon
        try:
            # Determine base path (PyInstaller bundle or development)
            if getattr(sys, "frozen", False):
                # Running as compiled executable - check bundle directory first
                base_path = Path(sys._MEIPASS)  # PyInstaller temp directory
            else:
                base_path = Path(__file__).parent

            # Try multiple locations for the icon file
            icon_paths = [
                base_path / "app_icon.ico",  # Bundle directory or development
                Path(sys.executable).parent / "app_icon.ico",  # Executable directory
                Path.cwd() / "app_icon.ico",  # Current working directory
            ]
            for icon_path in icon_paths:
                if icon_path.exists():
                    self.root.iconbitmap(str(icon_path))
                    break
        except (TclError, AttributeError):
            pass  # Icon not found or unsupported format - use default

        # Thread management
        self.processing_thread = None
        self.shutdown_event = threading.Event()

        # Configure ttk styles
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self.style.configure("Main.TFrame", background="#f0f0f0")
        self.style.configure(
            "Header.TLabel", font=("Segoe UI", 16, "bold"), background="#f0f0f0", foreground="#2c3e50", padding=10
        )
        self.style.configure(
            "Status.TLabel", font=("Segoe UI", 10), background="#f0f0f0", foreground="#7f8c8d", padding=5
        )
        self.style.configure("Modern.TButton", font=("Segoe UI", 10), padding=10)

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.create_widgets()

    def create_widgets(self):
        """Create all GUI widgets"""
        # Main container
        container = ttk.Frame(self.root, style="Main.TFrame")
        container.pack(fill=tk.BOTH, expand=True, pady=10, padx=10)

        # Header
        header_frame = ttk.Frame(container, style="Main.TFrame")
        header_frame.pack(fill=tk.X, pady=(0, 10))

        header_label = ttk.Label(header_frame, text="835 EDI File Processor", style="Header.TLabel")
        header_label.pack(side=tk.LEFT)

        # Status
        self.status_label = ttk.Label(header_frame, text="Ready to process files", style="Status.TLabel")
        self.status_label.pack(side=tk.RIGHT)

        # Progress frame with detailed status
        progress_frame = ttk.Frame(container, style="Main.TFrame")
        progress_frame.pack(fill=tk.X, pady=(0, 10))

        # Current operation label (stays visible at top)
        self.operation_label = ttk.Label(
            progress_frame, text="", font=("Segoe UI", 9, "bold"), background="#f0f0f0", foreground="#27ae60"
        )
        self.operation_label.pack(fill=tk.X, pady=(0, 5))

        # Text output area
        main_frame = ttk.Frame(container, style="Main.TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.text_output = ScrolledText(
            main_frame,
            font=("Consolas", 9),
            background="#ffffff",
            foreground="#2c3e50",
            wrap=tk.WORD,
            height=25,
            relief="flat",
            padx=10,
            pady=10,
            insertbackground="#2c3e50",
            selectbackground="#bdc3c7",
            selectforeground="#ffffff",
        )
        self.text_output.pack(fill=tk.BOTH, expand=True)

        # Progress bar
        self.progress = ttk.Progressbar(progress_frame, mode="indeterminate")
        self.progress.pack(fill=tk.X)

        # Redaction Mode Checkbox
        redaction_frame = ttk.Frame(container, style="Main.TFrame")
        redaction_frame.pack(fill=tk.X, pady=10)

        self.redaction_enabled = tk.BooleanVar(value=False)
        self.redaction_checkbox = ttk.Checkbutton(
            redaction_frame,
            text="Enable Testing Mode (Redact Names & IDs - Creates _testing folder)",
            variable=self.redaction_enabled,
        )
        self.redaction_checkbox.pack(side=tk.LEFT, padx=5)

        # Buttons
        button_frame = ttk.Frame(container, style="Main.TFrame")
        button_frame.pack(fill=tk.X)

        self.select_button = ttk.Button(
            button_frame, text="Select Folder", command=self.process_files, style="Modern.TButton"
        )
        self.select_button.pack(side=tk.LEFT, padx=5)

        self.clear_button = ttk.Button(
            button_frame, text="Clear Output", command=self.clear_output, style="Modern.TButton"
        )
        self.clear_button.pack(side=tk.LEFT, padx=5)

        self.settings_button = ttk.Button(
            button_frame, text="Settings", command=self.open_settings, style="Modern.TButton"
        )
        self.settings_button.pack(side=tk.LEFT, padx=5)

        self.db_report_button = ttk.Button(
            button_frame,
            text="DB Deductible Report",
            command=self.generate_db_deductible_report,
            style="Modern.TButton",
        )
        self.db_report_button.pack(side=tk.LEFT, padx=5)

        self.exit_button = ttk.Button(button_frame, text="Exit", command=self.on_closing, style="Modern.TButton")
        self.exit_button.pack(side=tk.RIGHT, padx=5)

        # Setup console redirection (both stdout and stderr)
        self.redirector = ConsoleRedirector(self.text_output, self.status_label, self.root, self.operation_label)
        sys.stdout = self.redirector
        sys.stderr = self.redirector

    def clear_output(self):
        """Clear the output text area"""
        self.text_output.delete("1.0", tk.END)
        self.status_label.config(text="Ready to process files")
        self.operation_label.config(text="")

    def open_settings(self):
        """Open the settings dialog"""
        from config import get_config

        config = get_config()
        dialog = SettingsDialog(self.root, config)
        if dialog.show():
            print("Settings saved successfully.\n")

    def generate_db_deductible_report(self):
        """Generate deductible report from database"""
        from config import get_config
        from database import get_default_db_path

        config = get_config()

        # Check if database exists - use configured path first, then fall back to default
        configured_db_path = config.get("database_path")
        if configured_db_path:
            db_path = Path(configured_db_path)
        else:
            db_path = get_default_db_path()

        if not db_path.exists():
            messagebox.showerror(
                "Database Not Found",
                f"No database found at:\n{db_path}\n\n" "Please process 835 files first to populate the database.",
            )
            return

        # Check if Fair Health ZIP CSV is configured (optional - proceeds silently without it)
        trips_path = config.get("trips_csv_path")
        if not trips_path or not Path(trips_path).exists():
            trips_path = None  # Proceed without patient payments (will show $0)

        # Use configured output directory or prompt user
        output_dir = config.get("deductible_report_output_dir")
        if output_dir:
            output_dir = os.path.expanduser(output_dir)
            # Verify directory exists
            if not Path(output_dir).exists():
                messagebox.showerror(
                    "Directory Not Found",
                    f"Configured output directory not found:\n{output_dir}\n\n"
                    "Please update Settings or create the directory.",
                )
                return
        else:
            # Prompt for output directory
            output_dir = filedialog.askdirectory(
                title="Select Output Directory for Deductible Reports", initialdir=Path.home() / "Desktop"
            )
            if not output_dir:
                print("No output directory selected.")
                return

        # Disable button and start progress BEFORE thread starts (prevents race condition)
        self.db_report_button.config(state=tk.DISABLED)
        self.status_label.config(text="Generating deductible reports...")
        self.progress.start()

        # Run in background thread - pass the resolved db_path
        self.processing_thread = threading.Thread(
            target=self._generate_db_report_thread, args=(trips_path, output_dir, str(db_path)), daemon=False
        )
        self.processing_thread.start()

    def _generate_db_report_thread(self, trips_path, output_dir, db_path=None):
        """Background thread for database report generation"""
        try:
            if self.shutdown_event.is_set():
                self._cleanup_db_report_ui()
                return

            # Import and run the report generator
            from generate_deductible_collection_reports import generate_from_database

            result = generate_from_database(trips_path, output_dir, db_path=db_path)

            # Update GUI on completion
            try:
                self.root.after(0, lambda: self.progress.stop())
                self.root.after(0, lambda: self.db_report_button.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.status_label.config(text="Reports generated successfully!"))
                self.root.after(0, lambda: self.operation_label.config(text=f"Output: {result}"))
            except tk.TclError:
                return

        except Exception as e:
            self._handle_processing_error(e, "Report generation error")
            self._cleanup_db_report_ui()

    def _cleanup_db_report_ui(self):
        """Re-enable DB report button and stop progress on completion/error"""
        try:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.db_report_button.config(state=tk.NORMAL))
        except tk.TclError:
            pass

    def process_files(self):
        """Process 835 files in selected folder"""
        folder_path = filedialog.askdirectory(title="Select Folder Containing 835 Files")

        if not folder_path:
            print("No folder selected.")
            return

        # Run processing in background thread to keep GUI responsive
        self.processing_thread = threading.Thread(target=self._process_files_thread, args=(folder_path,), daemon=False)
        self.processing_thread.start()

    def _process_files_thread(self, folder_path):
        """Background thread for processing files - keeps GUI responsive"""
        try:
            # Check if shutdown was requested
            if self.shutdown_event.is_set():
                return

            # Import process_folder from parser module
            # Note: Import is done here to avoid circular import when gui.py is imported from parser_835.py
            from parser_835 import process_folder

            # Safe GUI updates - check if window still exists
            try:
                self.root.after(0, lambda: self.status_label.config(text="Processing files..."))
                self.root.after(0, lambda: self.select_button.config(state=tk.DISABLED))
                self.root.after(0, lambda: self.progress.start())
            except tk.TclError:
                # Window was destroyed, abort processing
                return

            print(f"Processing 835 files in: {folder_path}\n")

            # Get redaction mode status
            enable_redaction = self.redaction_enabled.get()
            if enable_redaction:
                print("🔒 TESTING MODE ENABLED: Redacting names and IDs\n")

            # Create status callback for GUI updates
            def update_status(message):
                self.root.after(0, lambda m=message: self.operation_label.config(text=m))
                # Also update status label for key milestones
                self.root.after(
                    0, lambda m=message: self.status_label.config(text=m[:50] + "..." if len(m) > 50 else m)
                )

            result = process_folder(folder_path, enable_redaction, status_callback=update_status)

            # Ensure GUI updates happen on main thread (with TclError protection)
            try:
                self.root.after(0, lambda: self.progress.stop())
                self.root.after(0, lambda: self.select_button.config(state=tk.NORMAL))
            except tk.TclError:
                return  # Window was destroyed

            # Update status labels based on result
            # Check if there were any warnings during processing (from failed files)
            def update_final_status():
                current_op = self.operation_label.cget("text")
                if result:
                    # If operation label shows a warning, keep it; otherwise show success
                    if "Warning" in current_op or "failed" in current_op.lower():
                        self.status_label.config(text="⚠ Processing completed with errors")
                        # Keep the warning message in operation_label
                    else:
                        self.status_label.config(text="✅ All processing complete!")
                        self.operation_label.config(text="All processing complete!")
                else:
                    self.status_label.config(text="⚠ No data extracted")
                    self.operation_label.config(text="No data extracted")

            try:
                self.root.after(0, update_final_status)
            except tk.TclError:
                return  # Window was destroyed

        except OSError as e:
            # File access errors (missing files, permissions, disk full, etc.)
            self._handle_processing_error(e, "File access error")
        except (ValueError, KeyError, IndexError) as e:
            # Data parsing/format errors
            self._handle_processing_error(e, "Data format error")
        except ImportError as e:
            # Missing dependencies
            self._handle_processing_error(e, "Missing dependency")
        except Exception as e:
            # Catch-all for background thread: prevents thread crash and ensures GUI cleanup
            # This is intentionally broad as it's the last line of defense in a background thread
            self._handle_processing_error(e, "Unexpected error")

    def _handle_processing_error(self, error, error_category):
        """Handle processing errors with safe GUI updates and PHI-safe logging."""
        # Safely update GUI with TclError protection
        try:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.select_button.config(state=tk.NORMAL))
        except tk.TclError:
            return  # Window was destroyed

        # Print error without PHI (no full traceback with variable values)
        error_msg = f"\n{'='*80}\n{error_category.upper()}\n{'='*80}\n"
        error_msg += f"Error Type: {type(error).__name__}\n"
        error_msg += f"Error Message: {str(error)}\n"
        error_msg += "\nIf this error persists, please contact BCABC support.\n"
        print(error_msg)
        print(f"{'='*80}\n")

        # Log full traceback to file (not shown in GUI to avoid PHI exposure)
        try:
            import logging

            logging.error("Processing error: %s", error_category, exc_info=True)
        except Exception:
            pass  # If logging fails, don't crash

        try:
            self.root.after(0, lambda: self.status_label.config(text="❌ Error during processing"))
            self.root.after(0, lambda: self.operation_label.config(text="See error details above"))
        except tk.TclError:
            return  # Window was destroyed

    def run(self):
        """Start the GUI main loop"""
        try:
            # Center window
            self.root.update_idletasks()
            width = self.root.winfo_width()
            height = self.root.winfo_height()
            x = (self.root.winfo_screenwidth() // 2) - (width // 2)
            y = (self.root.winfo_screenheight() // 2) - (height // 2)
            self.root.geometry(f"{width}x{height}+{x}+{y}")

            # Check for first-run setup
            config, needs_setup = check_first_run_config()
            if needs_setup:
                # Prompt user to configure settings on first run
                result = messagebox.askyesno(
                    "First-Time Setup",
                    "Welcome to 835 EDI File Processor!\n\n"
                    "No configuration file was found. Would you like to configure "
                    "the paths to Fair Health ZIP CSV and RATES.xlsx now?\n\n"
                    "These files are optional but enable additional data enrichment features.",
                    icon="info",
                )
                if result:
                    self.open_settings()
                else:
                    print("Tip: You can configure file paths anytime via the Settings button.\n")

            self.root.mainloop()
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
        finally:
            # Restore stdout/stderr to originals
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            try:
                self.root.destroy()
            except TclError:
                pass
            # Return control to caller instead of exiting

    def on_closing(self):
        """Handle window close event - wait for processing to finish"""
        # Check if processing is running
        if self.processing_thread and self.processing_thread.is_alive():
            response = messagebox.askyesno(
                "Processing In Progress",
                "File processing is still running. Closing now may result in incomplete output files.\n\n"
                "Do you want to close anyway?",
                icon="warning",
            )
            if not response:
                return  # User chose to wait

            # User chose to force close - signal shutdown
            self.shutdown_event.set()
            print("\n⚠ Shutdown requested. Waiting for current file to finish...\n")

            # Give thread 3 seconds to finish current operation
            self.processing_thread.join(timeout=3.0)
            if self.processing_thread.is_alive():
                print("⚠ Warning: Processing thread did not stop cleanly. Files may be incomplete.\n")

        # Restore stdout/stderr to originals
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        self.root.quit()
        self.root.destroy()

        # Force exit if threads are still running (prevents zombie processes)
        if self.processing_thread and self.processing_thread.is_alive():
            import os

            os._exit(0)  # Force terminate - threads won't stop gracefully


def main():
    """Main entry point for GUI"""
    app = ProcessingWindow()
    app.run()


if __name__ == "__main__":
    main()

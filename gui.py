"""
835 EDI Parser - GUI Module

This module contains the graphical user interface for the 835 parser:
- ConsoleRedirector: Redirects console output to GUI text widget
- SettingsDialog: Configuration dialog for file paths
- ProcessingWindow: Main GUI window for processing 835 files
"""

import os
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, TclError
from tkinter.scrolledtext import ScrolledText
import threading
from pathlib import Path


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
        if self.operation_label and '[' in text and ']' in text:
            # Extract operation from text like "[1/9] Parsing EDI structure..."
            try:
                if text.strip().startswith('['):
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
        self.dialog.geometry("600x300")
        self.dialog.resizable(False, False)
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Center on parent
        self.dialog.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() - 600) // 2
        y = parent.winfo_y() + (parent.winfo_height() - 300) // 2
        self.dialog.geometry(f"+{x}+{y}")
        
        self._create_widgets()
        
    def _create_widgets(self):
        """Create dialog widgets"""
        # Main frame with padding
        main_frame = ttk.Frame(self.dialog, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, 
                               text="Configure File Paths",
                               font=('Segoe UI', 12, 'bold'))
        title_label.pack(pady=(0, 15))
        
        # Description
        desc_label = ttk.Label(main_frame,
                              text="Set the paths to external data files used for enrichment.\n"
                                   "These files are optional - leave blank if not available.",
                              font=('Segoe UI', 9),
                              foreground='#666666')
        desc_label.pack(pady=(0, 15))
        
        # Trips.csv path
        trips_frame = ttk.Frame(main_frame)
        trips_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(trips_frame, text="Trips.csv:", width=12).pack(side=tk.LEFT)
        self.trips_var = tk.StringVar(value=self.config.get('trips_csv_path') or '')
        trips_entry = ttk.Entry(trips_frame, textvariable=self.trips_var, width=50)
        trips_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(trips_frame, text="Browse...", 
                  command=lambda: self._browse_file(self.trips_var, "Trips.csv", [("CSV files", "*.csv")])).pack(side=tk.LEFT)
        
        # RATES.xlsx path
        rates_frame = ttk.Frame(main_frame)
        rates_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(rates_frame, text="RATES.xlsx:", width=12).pack(side=tk.LEFT)
        self.rates_var = tk.StringVar(value=self.config.get('rates_xlsx_path') or '')
        rates_entry = ttk.Entry(rates_frame, textvariable=self.rates_var, width=50)
        rates_entry.pack(side=tk.LEFT, padx=5)
        ttk.Button(rates_frame, text="Browse...",
                  command=lambda: self._browse_file(self.rates_var, "RATES.xlsx", [("Excel files", "*.xlsx")])).pack(side=tk.LEFT)
        
        # Info about config file
        info_label = ttk.Label(main_frame,
                              text="Settings are saved to 835_config.json in the application directory.",
                              font=('Segoe UI', 8),
                              foreground='#999999')
        info_label.pack(pady=(20, 10))
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=10)
        
        ttk.Button(button_frame, text="Save", command=self._save, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self._cancel, width=10).pack(side=tk.LEFT, padx=5)
        
    def _browse_file(self, var, title, filetypes):
        """Open file browser dialog"""
        initial_dir = os.path.dirname(var.get()) if var.get() else os.path.expanduser("~")
        filepath = filedialog.askopenfilename(
            title=f"Select {title}",
            initialdir=initial_dir,
            filetypes=filetypes + [("All files", "*.*")]
        )
        if filepath:
            var.set(filepath)
            
    def _save(self):
        """Save settings and close dialog"""
        # Update config
        trips_path = self.trips_var.get().strip()
        rates_path = self.rates_var.get().strip()
        
        self.config.set('trips_csv_path', trips_path if trips_path else None)
        self.config.set('rates_xlsx_path', rates_path if rates_path else None)
        
        # Try multiple writable locations in order of preference
        save_locations = [
            Path.cwd() / '835_config.json',                                    # Current directory
            Path.home() / '835_config.json',                                   # User home directory
            Path(os.path.dirname(os.path.abspath(__file__))) / '835_config.json'  # App directory
        ]
        
        for config_path in save_locations:
            try:
                self.config.save(str(config_path))
                self.result = True
                self.dialog.destroy()
                return
            except (PermissionError, OSError):
                continue
        
        # All locations failed
        messagebox.showerror("Error", "Could not save config to any location. Check write permissions.")
            
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
    trips_path = config.get('trips_csv_path')
    rates_path = config.get('rates_xlsx_path')
    
    needs_setup = (not trips_path) and (not rates_path)
    return config, needs_setup


class ProcessingWindow:
    """Main GUI window for processing 835 files"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("835 File Processor")
        self.root.geometry("1000x700")
        self.root.configure(background='#f0f0f0')

        # Configure ttk styles
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure('Main.TFrame', background='#f0f0f0')
        self.style.configure('Header.TLabel',
                           font=('Segoe UI', 16, 'bold'),
                           background='#f0f0f0',
                           foreground='#2c3e50',
                           padding=10)
        self.style.configure('Status.TLabel',
                           font=('Segoe UI', 10),
                           background='#f0f0f0',
                           foreground='#7f8c8d',
                           padding=5)
        self.style.configure('Modern.TButton',
                           font=('Segoe UI', 10),
                           padding=10)

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.create_widgets()

    def create_widgets(self):
        """Create all GUI widgets"""
        # Main container
        container = ttk.Frame(self.root, style='Main.TFrame')
        container.pack(fill=tk.BOTH, expand=True, pady=10, padx=10)

        # Header
        header_frame = ttk.Frame(container, style='Main.TFrame')
        header_frame.pack(fill=tk.X, pady=(0, 10))

        header_label = ttk.Label(header_frame,
                                text="835 EDI File Processor",
                                style='Header.TLabel')
        header_label.pack(side=tk.LEFT)

        # Status
        self.status_label = ttk.Label(header_frame,
                                     text="Ready to process files",
                                     style='Status.TLabel')
        self.status_label.pack(side=tk.RIGHT)

        # Progress frame with detailed status
        progress_frame = ttk.Frame(container, style='Main.TFrame')
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Current operation label (stays visible at top)
        self.operation_label = ttk.Label(progress_frame,
                                        text="",
                                        font=('Segoe UI', 9, 'bold'),
                                        background='#f0f0f0',
                                        foreground='#27ae60')
        self.operation_label.pack(fill=tk.X, pady=(0, 5))

        # Text output area
        main_frame = ttk.Frame(container, style='Main.TFrame')
        main_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.text_output = ScrolledText(main_frame,
                                       font=('Consolas', 9),
                                       background='#ffffff',
                                       foreground='#2c3e50',
                                       wrap=tk.WORD,
                                       height=25,
                                       relief='flat',
                                       padx=10,
                                       pady=10,
                                       insertbackground='#2c3e50',
                                       selectbackground='#bdc3c7',
                                       selectforeground='#ffffff')
        self.text_output.pack(fill=tk.BOTH, expand=True)

        # Progress bar
        self.progress = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X)

        # Redaction Mode Checkbox
        redaction_frame = ttk.Frame(container, style='Main.TFrame')
        redaction_frame.pack(fill=tk.X, pady=10)
        
        self.redaction_enabled = tk.BooleanVar(value=False)
        self.redaction_checkbox = ttk.Checkbutton(redaction_frame,
                                                  text="Enable Testing Mode (Redact Names & IDs - Creates _testing folder)",
                                                  variable=self.redaction_enabled)
        self.redaction_checkbox.pack(side=tk.LEFT, padx=5)

        # Buttons
        button_frame = ttk.Frame(container, style='Main.TFrame')
        button_frame.pack(fill=tk.X)

        self.select_button = ttk.Button(button_frame,
                                       text="Select Folder",
                                       command=self.process_files,
                                       style='Modern.TButton')
        self.select_button.pack(side=tk.LEFT, padx=5)

        self.clear_button = ttk.Button(button_frame,
                                      text="Clear Output",
                                      command=self.clear_output,
                                      style='Modern.TButton')
        self.clear_button.pack(side=tk.LEFT, padx=5)

        self.settings_button = ttk.Button(button_frame,
                                         text="Settings",
                                         command=self.open_settings,
                                         style='Modern.TButton')
        self.settings_button.pack(side=tk.LEFT, padx=5)

        self.exit_button = ttk.Button(button_frame,
                                     text="Exit",
                                     command=self.on_closing,
                                     style='Modern.TButton')
        self.exit_button.pack(side=tk.RIGHT, padx=5)

        # Setup console redirection (both stdout and stderr)
        self.redirector = ConsoleRedirector(self.text_output, self.status_label, self.root, self.operation_label)
        sys.stdout = self.redirector
        sys.stderr = self.redirector

    def clear_output(self):
        """Clear the output text area"""
        self.text_output.delete('1.0', tk.END)
        self.status_label.config(text="Ready to process files")
        self.operation_label.config(text="")

    def open_settings(self):
        """Open the settings dialog"""
        from config import get_config
        config = get_config()
        dialog = SettingsDialog(self.root, config)
        if dialog.show():
            print("Settings saved successfully.\n")

    def process_files(self):
        """Process 835 files in selected folder"""
        folder_path = filedialog.askdirectory(title="Select Folder Containing 835 Files")

        if not folder_path:
            print("No folder selected.")
            return

        # Run processing in background thread to keep GUI responsive
        processing_thread = threading.Thread(target=self._process_files_thread, args=(folder_path,), daemon=True)
        processing_thread.start()
    
    def _process_files_thread(self, folder_path):
        """Background thread for processing files - keeps GUI responsive"""
        try:
            # Import process_folder from parser module
            # Note: Import is done here to avoid circular import when gui.py is imported from parser_835.py
            from parser_835 import process_folder
            
            self.root.after(0, lambda: self.status_label.config(text="Processing files..."))
            self.root.after(0, lambda: self.select_button.config(state=tk.DISABLED))
            self.root.after(0, lambda: self.progress.start())

            print(f"Processing 835 files in: {folder_path}\n")
            
            # Get redaction mode status
            enable_redaction = self.redaction_enabled.get()
            if enable_redaction:
                print("🔒 TESTING MODE ENABLED: Redacting names and IDs\n")

            # Create status callback for GUI updates
            def update_status(message):
                self.root.after(0, lambda m=message: self.operation_label.config(text=m))
                # Also update status label for key milestones
                self.root.after(0, lambda m=message: self.status_label.config(text=m[:50] + "..." if len(m) > 50 else m))
            
            result = process_folder(folder_path, enable_redaction, status_callback=update_status)

            # Ensure GUI updates happen on main thread
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.select_button.config(state=tk.NORMAL))

            # Update status labels based on result
            if result:
                self.root.after(0, lambda: self.status_label.config(text="✅ All processing complete!"))
                self.root.after(0, lambda: self.operation_label.config(text="All processing complete!"))
            else:
                self.root.after(0, lambda: self.status_label.config(text="⚠ No data extracted"))
                self.root.after(0, lambda: self.operation_label.config(text="No data extracted"))

        except Exception as e:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.select_button.config(state=tk.NORMAL))

            # Print detailed error with full traceback to GUI
            import traceback
            error_msg = f"\n{'='*80}\nERROR DURING PROCESSING\n{'='*80}\n"
            error_msg += f"Error Type: {type(e).__name__}\n"
            error_msg += f"Error Message: {str(e)}\n"
            error_msg += f"\nFull Traceback:\n{'-'*80}\n"
            print(error_msg)
            traceback.print_exc()  # Now goes to stderr which is redirected to GUI
            print(f"{'='*80}\n")

            self.root.after(0, lambda: self.status_label.config(text="❌ Error during processing"))
            self.root.after(0, lambda: self.operation_label.config(text="See error details above"))

    def run(self):
        """Start the GUI main loop"""
        try:
            # Center window
            self.root.update_idletasks()
            width = self.root.winfo_width()
            height = self.root.winfo_height()
            x = (self.root.winfo_screenwidth() // 2) - (width // 2)
            y = (self.root.winfo_screenheight() // 2) - (height // 2)
            self.root.geometry(f'{width}x{height}+{x}+{y}')

            # Check for first-run setup
            config, needs_setup = check_first_run_config()
            if needs_setup:
                # Prompt user to configure settings on first run
                result = messagebox.askyesno(
                    "First-Time Setup",
                    "Welcome to 835 EDI File Processor!\n\n"
                    "No configuration file was found. Would you like to configure "
                    "the paths to Trips.csv and RATES.xlsx now?\n\n"
                    "These files are optional but enable additional data enrichment features.",
                    icon='info'
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
        """Handle window close event"""
        # Restore stdout/stderr to originals
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        self.root.quit()
        self.root.destroy()
        # Return control to caller instead of exiting


def main():
    """Main entry point for GUI"""
    app = ProcessingWindow()
    app.run()


if __name__ == '__main__':
    main()


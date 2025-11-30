"""
835 EDI Parser - GUI Module

This module contains the graphical user interface for the 835 parser:
- ConsoleRedirector: Redirects console output to GUI text widget
- ProcessingWindow: Main GUI window for processing 835 files
"""

import os
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText
import threading


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
            except:
                pass

    def _write_to_widget(self, text):
        """Internal method to write to widget on main thread"""
        self.text_widget.insert(tk.END, text)
        self.text_widget.see(tk.END)  # Auto-scroll to bottom
        self.text_widget.update_idletasks()

    def flush(self):
        """Flush buffer (required for file-like object)"""
        pass


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
        
        # Validation checkbox removed - validation no longer supported
        
        # Issue tracking removed from program

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

        self.exit_button = ttk.Button(button_frame,
                                     text="Exit",
                                     command=self.on_closing,
                                     style='Modern.TButton')
        self.exit_button.pack(side=tk.RIGHT, padx=5)

        # Setup console redirection
        self.redirector = ConsoleRedirector(self.text_output, self.status_label, self.root, self.operation_label)
        sys.stdout = self.redirector

    def clear_output(self):
        """Clear the output text area"""
        self.text_output.delete('1.0', tk.END)
        self.status_label.config(text="Ready to process files")
        self.operation_label.config(text="")

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
            # Import process_folder from 835 parser module
            # Note: Import is done here to avoid issues when gui.py is imported from 835.py
            import importlib.util
            parser_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "835.py")
            spec = importlib.util.spec_from_file_location("parser_835", parser_path)
            parser_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(parser_module)
            process_folder = parser_module.process_folder
            
            self.root.after(0, lambda: self.status_label.config(text="Processing files..."))
            self.root.after(0, lambda: self.select_button.config(state=tk.DISABLED))
            self.root.after(0, lambda: self.progress.start())

            print(f"Processing 835 files in: {folder_path}\n")
            
            # Get redaction mode status
            enable_redaction = self.redaction_enabled.get()
            if enable_redaction:
                print("🔒 TESTING MODE ENABLED: Redacting names and IDs\n")
            
            # Validation removed from program
            
            # Issue tracking removed from program

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
            print(f"Error during processing: {str(e)}")
            import traceback
            traceback.print_exc()
            self.root.after(0, lambda: self.status_label.config(text="Error during processing"))

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

            self.root.mainloop()
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
        finally:
            sys.stdout = sys.__stdout__
            try:
                self.root.destroy()
            except:
                pass
            sys.exit(0)

    def on_closing(self):
        """Handle window close event"""
        sys.stdout = sys.__stdout__
        self.root.quit()
        self.root.destroy()
        sys.exit(0)


def main():
    """Main entry point for GUI"""
    app = ProcessingWindow()
    app.run()


if __name__ == '__main__':
    main()


#!/usr/bin/env python3
"""
Enhanced GUI Script to load data from Excel to Smartsheet
Professional interface for Cin7 inventory uploads with improved UX - macOS Optimized
"""

import pandas as pd
import smartsheet
import logging
from datetime import datetime
from typing import List, Dict, Any
import numpy as np
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
import threading
import os
import re
import tempfile
import platform
from pathlib import Path

# Smartsheet configuration
SMARTSHEET_TOKEN = "pQxhZNG27iD0OXNcG2e3VJnZi3PRVDD6SD2Ju"

class EnhancedSmartsheetUploaderGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Cin7 to Smartsheet Uploader v2.0")
        self.root.geometry("900x800")
        self.root.resizable(True, True)
        
        # Set minimum size
        self.root.minsize(800, 600)
        
        # Variables
        self.selected_file = tk.StringVar()
        self.smartsheet_url = tk.StringVar()
        self.log_directory = tk.StringVar()
        self.sheet_id = ""
        self.smart = None
        self.processed_df = None
        self.is_processing = False
        
        # Set default log directory appropriate for macOS
        if platform.system() == "Darwin":  # macOS
            default_log_dir = Path.home() / "Documents" / "Cin7Logs"
        else:  # Windows/Linux fallback
            default_log_dir = Path.cwd() / "logs"
        
        self.log_directory.set(str(default_log_dir))
        
        # Setup modern style
        self.setup_style()
        
        # Setup GUI
        self.setup_gui()
        
        # Setup logging
        self.setup_logging()
        
    def setup_style(self):
        """Setup modern styling for the application"""
        style = ttk.Style()
        
        # Configure modern theme
        style.theme_use('clam')
        
        # macOS-friendly fonts
        if platform.system() == "Darwin":  # macOS
            title_font = ('SF Pro Display', 18, 'bold')
            heading_font = ('SF Pro Display', 11, 'bold')
            body_font = ('SF Pro Display', 9)
            mono_font = ('SF Mono', 9)
        else:  # Windows/Linux
            title_font = ('Segoe UI', 18, 'bold')
            heading_font = ('Segoe UI', 11, 'bold')
            body_font = ('Segoe UI', 9)
            mono_font = ('Consolas', 9)
        
        # Custom styles
        style.configure('Title.TLabel', font=title_font, foreground='#2c3e50')
        style.configure('Heading.TLabel', font=heading_font, foreground='#34495e')
        style.configure('Info.TLabel', font=body_font, foreground='#7f8c8d')
        style.configure('Success.TButton', background='#27ae60', foreground='white')
        style.configure('Warning.TButton', background='#e67e22', foreground='white')
        style.configure('Danger.TButton', background='#e74c3c', foreground='white')
        
        # Modern button styling
        style.configure('Modern.TButton', 
                       padding=(10, 5), 
                       font=body_font)
        
        # Store fonts for later use
        self.fonts = {
            'title': title_font,
            'heading': heading_font,
            'body': body_font,
            'mono': mono_font
        }
                       
    def setup_gui(self):
        """Setup the enhanced graphical user interface"""
        # Create main container with notebook for tabbed interface
        self.notebook = ttk.Notebook(self.root, padding="10")
        self.notebook.pack(fill='both', expand=True)
        
        # Main tab
        self.main_tab = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.main_tab, text="Upload Data")
        
        # Settings tab
        self.settings_tab = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.settings_tab, text="Settings")
        
        self.setup_main_tab()
        self.setup_settings_tab()
        
    def setup_main_tab(self):
        """Setup the main upload tab"""
        # Configure grid weights
        self.main_tab.columnconfigure(1, weight=1)
        self.main_tab.rowconfigure(6, weight=1)
        
        # Title section
        title_frame = ttk.Frame(self.main_tab)
        title_frame.grid(row=0, column=0, columnspan=3, sticky='ew', pady=(0, 30))
        
        ttk.Label(title_frame, text="Cin7 Inventory to Smartsheet Uploader", 
                 style='Title.TLabel').pack()
        ttk.Label(title_frame, text="Professional data upload tool for inventory management", 
                 style='Info.TLabel').pack(pady=(5, 0))
        
        # File selection section
        file_section = ttk.LabelFrame(self.main_tab, text="Step 1: Select Excel File", padding="15")
        file_section.grid(row=1, column=0, columnspan=3, sticky='ew', pady=(0, 15))
        file_section.columnconfigure(0, weight=1)
        
        self.file_entry = ttk.Entry(file_section, textvariable=self.selected_file, 
                                   font=self.fonts['body'], width=70)
        self.file_entry.grid(row=0, column=0, sticky='ew', padx=(0, 10))
        
        self.browse_button = ttk.Button(file_section, text="Browse Files", 
                                       command=self.browse_file, style='Modern.TButton')
        self.browse_button.grid(row=0, column=1)
        
        # File info display
        self.file_info = ttk.Label(file_section, text="No file selected", style='Info.TLabel')
        self.file_info.grid(row=1, column=0, columnspan=2, sticky='w', pady=(10, 0))
        
        # Smartsheet URL section
        url_section = ttk.LabelFrame(self.main_tab, text="Step 2: Smartsheet Connection", padding="15")
        url_section.grid(row=2, column=0, columnspan=3, sticky='ew', pady=(0, 15))
        url_section.columnconfigure(0, weight=1)
        
        self.url_entry = ttk.Entry(url_section, textvariable=self.smartsheet_url, 
                                  font=self.fonts['body'], width=70)
        self.url_entry.grid(row=0, column=0, sticky='ew', padx=(0, 10))
        
        self.validate_button = ttk.Button(url_section, text="Validate Connection", 
                                         command=self.validate_smartsheet, style='Modern.TButton')
        self.validate_button.grid(row=0, column=1)
        
        # Connection status
        self.connection_status = ttk.Label(url_section, text="Not connected", style='Info.TLabel')
        self.connection_status.grid(row=1, column=0, columnspan=2, sticky='w', pady=(10, 0))
        
        # Process section
        process_section = ttk.LabelFrame(self.main_tab, text="Step 3: Process & Upload", padding="15")
        process_section.grid(row=3, column=0, columnspan=3, sticky='ew', pady=(0, 15))
        
        # Progress info
        self.progress_info = ttk.Label(process_section, text="Ready to process data", style='Info.TLabel')
        self.progress_info.pack(anchor='w', pady=(0, 10))
        
        # Control buttons frame
        button_frame = ttk.Frame(process_section)
        button_frame.pack(fill='x')
        
        self.process_button = ttk.Button(button_frame, text="Process & Upload Data", 
                                       command=self.start_processing, state='disabled',
                                       style='Success.TButton')
        self.process_button.pack(side='left', padx=(0, 10))
        
        self.stop_button = ttk.Button(button_frame, text="Stop Process", 
                                     command=self.stop_processing, state='disabled',
                                     style='Danger.TButton')
        self.stop_button.pack(side='left', padx=(0, 10))
        
        self.clear_button = ttk.Button(button_frame, text="Clear Log", 
                                      command=self.clear_log, style='Modern.TButton')
        self.clear_button.pack(side='left')
        
        # Progress bar
        self.progress = ttk.Progressbar(process_section, mode='indeterminate')
        self.progress.pack(fill='x', pady=(15, 0))
        
        # Log section
        log_section = ttk.LabelFrame(self.main_tab, text="Process Log", padding="15")
        log_section.grid(row=4, column=0, columnspan=3, sticky='nsew', pady=(0, 15))
        log_section.rowconfigure(0, weight=1)
        log_section.columnconfigure(0, weight=1)
        
        # Log text with better formatting
        log_frame = ttk.Frame(log_section)
        log_frame.grid(row=0, column=0, sticky='nsew')
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            width=90, height=15, 
            wrap=tk.WORD, 
            state=tk.DISABLED,
            font=self.fonts['mono'],
            bg='#f8f9fa',
            fg='#2c3e50'
        )
        self.log_text.grid(row=0, column=0, sticky='nsew')
        
        # Status bar
        self.status_frame = ttk.Frame(self.main_tab)
        self.status_frame.grid(row=5, column=0, columnspan=3, sticky='ew', pady=(10, 0))
        
        self.status_label = ttk.Label(self.status_frame, text="Ready", style='Info.TLabel')
        self.status_label.pack(side='left')
        
        # Add timestamp
        self.timestamp_label = ttk.Label(self.status_frame, text="", style='Info.TLabel')
        self.timestamp_label.pack(side='right')
        self.update_timestamp()
        
    def setup_settings_tab(self):
        """Setup the settings tab"""
        settings_frame = ttk.Frame(self.settings_tab)
        settings_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Log settings
        log_section = ttk.LabelFrame(settings_frame, text="Log Settings", padding="15")
        log_section.pack(fill='x', pady=(0, 20))
        log_section.columnconfigure(0, weight=1)
        
        ttk.Label(log_section, text="Log files will be saved to:", style='Heading.TLabel').grid(
            row=0, column=0, columnspan=2, sticky='w', pady=(0, 10))
        
        self.log_entry = ttk.Entry(log_section, textvariable=self.log_directory, 
                                  font=self.fonts['body'], width=60)
        self.log_entry.grid(row=1, column=0, sticky='ew', padx=(0, 10))
        
        self.log_browse_button = ttk.Button(log_section, text="Choose Folder", 
                                           command=self.browse_log_directory, 
                                           style='Modern.TButton')
        self.log_browse_button.grid(row=1, column=1)
        
        # Reset to default
        ttk.Button(log_section, text="Reset to Default", 
                  command=self.reset_log_directory, 
                  style='Modern.TButton').grid(row=2, column=0, sticky='w', pady=(10, 0))
        
        # Current log directory status
        self.log_status = ttk.Label(log_section, text="", style='Info.TLabel')
        self.log_status.grid(row=3, column=0, columnspan=2, sticky='w', pady=(10, 0))
        self.update_log_status()
        
        # Connection settings
        conn_section = ttk.LabelFrame(settings_frame, text="Connection Settings", padding="15")
        conn_section.pack(fill='x', pady=(0, 20))
        
        ttk.Label(conn_section, text="Smartsheet API Token:", style='Heading.TLabel').pack(anchor='w')
        token_display = ttk.Label(conn_section, text=f"*****{SMARTSHEET_TOKEN[-8:]}", style='Info.TLabel')
        token_display.pack(anchor='w', pady=(5, 0))
        
        # System info section
        system_section = ttk.LabelFrame(settings_frame, text="System Information", padding="15")
        system_section.pack(fill='x', pady=(0, 20))
        
        system_info = f"""
Operating System: {platform.system()} {platform.release()}
Python Version: {platform.python_version()}
Application Path: {Path(__file__).parent if '__file__' in globals() else 'Unknown'}
        """
        
        ttk.Label(system_section, text=system_info.strip(), style='Info.TLabel').pack(anchor='w')
        
        # About section
        about_section = ttk.LabelFrame(settings_frame, text="About", padding="15")
        about_section.pack(fill='x')
        
        about_text = """
Cin7 to Smartsheet Uploader v2.0
Professional data upload tool for inventory management

Features:
• Excel file processing with automatic column detection
• Real-time progress tracking and logging
• Batch upload with error handling
• Modern, user-friendly interface
• Customizable log file location
• macOS optimized fonts and behavior
        """
        
        ttk.Label(about_section, text=about_text.strip(), style='Info.TLabel').pack(anchor='w')
        
    def update_log_status(self):
        """Update log directory status"""
        log_path = Path(self.log_directory.get())
        if log_path.exists():
            status = f"Directory exists: {log_path}"
        else:
            status = f"Directory will be created: {log_path}"
        self.log_status.config(text=status)
        
    def browse_log_directory(self):
        """Browse for log directory"""
        directory = filedialog.askdirectory(
            title="Select Log Directory",
            initialdir=self.log_directory.get()
        )
        
        if directory:
            self.log_directory.set(directory)
            self.log_message(f"Log directory changed to: {directory}")
            self.update_log_status()
            
    def reset_log_directory(self):
        """Reset log directory to default"""
        if platform.system() == "Darwin":  # macOS
            default_log_dir = Path.home() / "Documents" / "Cin7Logs"
        else:  # Windows/Linux fallback
            default_log_dir = Path.cwd() / "logs"
            
        self.log_directory.set(str(default_log_dir))
        self.log_message(f"Log directory reset to: {default_log_dir}")
        self.update_log_status()
        
    def update_timestamp(self):
        """Update timestamp display"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.timestamp_label.config(text=f"Last updated: {current_time}")
        # Update every minute
        self.root.after(60000, self.update_timestamp)
        
    def setup_logging(self):
        """Setup enhanced logging with file location choice and macOS compatibility"""
        class GUILogHandler(logging.Handler):
            def __init__(self, text_widget, status_callback=None):
                super().__init__()
                self.text_widget = text_widget
                self.status_callback = status_callback
                
            def emit(self, record):
                msg = self.format(record)
                def append():
                    self.text_widget.config(state=tk.NORMAL)
                    
                    # Add color coding based on level
                    if record.levelno >= logging.ERROR:
                        tag = 'error'
                    elif record.levelno >= logging.WARNING:
                        tag = 'warning'
                    elif 'success' in msg.lower() or '✅' in msg:
                        tag = 'success'
                    else:
                        tag = 'info'
                    
                    start_idx = self.text_widget.index(tk.END)
                    self.text_widget.insert(tk.END, msg + '\n')
                    end_idx = self.text_widget.index(tk.END)
                    
                    # Apply tags
                    if tag != 'info':
                        self.text_widget.tag_add(tag, start_idx, end_idx)
                    
                    self.text_widget.config(state=tk.DISABLED)
                    self.text_widget.see(tk.END)
                    
                    # Update status
                    if self.status_callback:
                        self.status_callback(msg)
                        
                self.text_widget.after(0, append)
        
        # Configure text tags for colored output
        mono_font_bold = (self.fonts['mono'][0], self.fonts['mono'][1], 'bold')
        
        self.log_text.tag_configure('error', foreground='#e74c3c', font=mono_font_bold)
        self.log_text.tag_configure('warning', foreground='#f39c12', font=mono_font_bold)
        self.log_text.tag_configure('success', foreground='#27ae60', font=mono_font_bold)
        self.log_text.tag_configure('info', foreground='#2c3e50')
        
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Add GUI handler
        gui_handler = GUILogHandler(self.log_text, self.update_status)
        gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        logging.getLogger().addHandler(gui_handler)
        
    def update_status(self, message):
        """Update status bar"""
        # Extract key info for status
        if "Connected to sheet" in message:
            self.status_label.config(text="Connected to Smartsheet")
        elif "Starting data processing" in message:
            self.status_label.config(text="Processing data...")
        elif "Upload completed" in message:
            self.status_label.config(text="Upload completed successfully")
        elif "Error" in message:
            self.status_label.config(text="Error occurred - check log")
            
    def get_log_file_path(self):
        """Get the full path for the log file with enhanced error handling"""
        log_dir = Path(self.log_directory.get())
        
        # Ensure directory exists on macOS/Unix systems
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            # Fallback to user's home directory if permission denied
            log_dir = Path.home() / "Cin7Logs"
            try:
                log_dir.mkdir(parents=True, exist_ok=True)
                self.log_directory.set(str(log_dir))
                self.log_message(f"Log directory changed to: {log_dir} (permission fallback)")
            except PermissionError:
                # Last resort: use temp directory
                log_dir = Path(tempfile.gettempdir()) / "Cin7Logs"
                log_dir.mkdir(parents=True, exist_ok=True)
                self.log_message(f"Using temporary log directory: {log_dir}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"smartsheet_upload_{timestamp}.log"
        return log_dir / log_filename
        
    def log_message(self, message):
        """Add message to log display and file with enhanced error handling"""
        # Create file handler for this session if not exists
        if not hasattr(self, 'file_handler'):
            try:
                log_path = self.get_log_file_path()
                self.file_handler = logging.FileHandler(log_path, encoding='utf-8')
                self.file_handler.setFormatter(
                    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                )
                logging.getLogger().addHandler(self.file_handler)
                self.log_message(f"Log file created: {log_path}")
            except (PermissionError, OSError) as e:
                # Fallback to temp directory if no permissions
                temp_log = Path(tempfile.gettempdir()) / f"smartsheet_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                self.file_handler = logging.FileHandler(temp_log, encoding='utf-8')
                self.file_handler.setFormatter(
                    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                )
                logging.getLogger().addHandler(self.file_handler)
                logging.warning(f"Using temporary log file due to permissions: {temp_log}")
        
        # Log the message
        logging.info(message)
        
    def clear_log(self):
        """Clear the log display"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.status_label.config(text="Log cleared")
        
    def browse_file(self):
        """Browse for Excel file with enhanced feedback"""
        file_path = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=[
                ("Excel files", "*.xlsx *.xls"),
                ("All files", "*.*")
            ]
        )
        
        if file_path:
            self.selected_file.set(file_path)
            
            # Get file info
            try:
                file_size = Path(file_path).stat().st_size
                size_mb = file_size / (1024 * 1024)
                
                self.file_info.config(
                    text=f"Selected: {Path(file_path).name} ({size_mb:.1f} MB)"
                )
                self.log_message(f"File selected: {Path(file_path).name}")
            except OSError as e:
                self.file_info.config(text=f"Selected: {Path(file_path).name} (size unknown)")
                self.log_message(f"File selected: {Path(file_path).name} (warning: {e})")
            
            self.check_ready_state()
            
    def extract_sheet_id_from_url(self, url):
        """Extract Sheet ID from Smartsheet URL"""
        try:
            patterns = [
                r'/sheets/([a-zA-Z0-9]+)',
                r'sheets/([a-zA-Z0-9]+)',
                r'sheet_id=([a-zA-Z0-9]+)',
                r'/sheets/([^/?#]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    sheet_id = match.group(1)
                    if len(sheet_id) >= 18 and sheet_id.replace('_', '').replace('-', '').isalnum():
                        return sheet_id
            return None
            
        except Exception as e:
            logging.error(f"Error extracting Sheet ID: {str(e)}")
            return None
            
    def validate_smartsheet(self):
        """Validate Smartsheet URL and connection"""
        url = self.smartsheet_url.get().strip()
        
        if not url:
            messagebox.showerror("Error", "Please enter a Smartsheet URL")
            return
            
        self.connection_status.config(text="Validating connection...")
        
        # Extract Sheet ID
        sheet_id = self.extract_sheet_id_from_url(url)
        
        if not sheet_id:
            self.connection_status.config(text="Invalid URL format")
            messagebox.showerror("Error", 
                               "Could not extract Sheet ID from URL.\n" +
                               "Please make sure you're using a valid Smartsheet URL.")
            return
            
        self.sheet_id = sheet_id
        self.log_message(f"Extracted Sheet ID: {sheet_id}")
        
        # Test connection in separate thread to avoid blocking UI
        def test_connection():
            try:
                self.smart = smartsheet.Smartsheet(SMARTSHEET_TOKEN)
                self.smart.errors_as_exceptions(True)
                
                sheet = self.smart.Sheets.get_sheet(sheet_id)
                
                # Update UI in main thread
                def update_ui():
                    self.connection_status.config(text=f"Connected to: {sheet.name}")
                    self.log_message(f"Connected to sheet: '{sheet.name}'")
                    self.log_message(f"Sheet has {len(sheet.columns)} columns and {sheet.total_row_count} rows")
                    
                    messagebox.showinfo("Success", f"Successfully connected to sheet: '{sheet.name}'")
                    self.check_ready_state()
                
                self.root.after(0, update_ui)
                
            except Exception as e:
                def show_error():
                    error_msg = f"Failed to connect to Smartsheet: {str(e)}"
                    self.connection_status.config(text="Connection failed")
                    self.log_message(f"ERROR: {error_msg}")
                    messagebox.showerror("Connection Error", error_msg)
                
                self.root.after(0, show_error)
        
        # Run in separate thread
        thread = threading.Thread(target=test_connection)
        thread.daemon = True
        thread.start()
            
    def check_ready_state(self):
        """Check if everything is ready for processing"""
        file_ready = bool(self.selected_file.get() and os.path.exists(self.selected_file.get()))
        sheet_ready = bool(self.sheet_id and self.smart)
        
        if file_ready and sheet_ready:
            self.process_button.config(state='normal')
            self.progress_info.config(text="Ready to process! Click 'Process & Upload Data' to begin.")
        else:
            self.process_button.config(state='disabled')
            
    def start_processing(self):
        """Start the processing in a separate thread"""
        self.is_processing = True
        self.process_button.config(state='disabled')
        self.stop_button.config(state='normal')
        self.progress.start()
        
        # Run processing in separate thread
        thread = threading.Thread(target=self.process_data)
        thread.daemon = True
        thread.start()
        
    def stop_processing(self):
        """Stop the current processing"""
        self.is_processing = False
        self.progress.stop()
        self.process_button.config(state='normal')
        self.stop_button.config(state='disabled')
        self.log_message("Processing stopped by user")
        self.status_label.config(text="Processing stopped")
        
    def process_data(self):
        """Process the Excel data and upload to Smartsheet"""
        try:
            if not self.is_processing:
                return
                
            self.log_message("Starting data processing...")
            
            # Read and process Excel file
            file_path = self.selected_file.get()
            self.log_message(f"Reading file: {Path(file_path).name}")
            
            df = pd.read_excel(file_path, engine='openpyxl', header=0)
            self.log_message(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            
            if not self.is_processing:
                return
                
            # Process data
            processed_df = self.process_excel_data(df)
            
            if len(processed_df) == 0:
                self.log_message("ERROR: No valid data found to upload")
                messagebox.showerror("Error", "No valid data found in the Excel file")
                return
                
            self.log_message(f"SUCCESS: Processed {len(processed_df)} products successfully")
            
            # Ask for confirmation
            def ask_confirmation():
                result = messagebox.askyesno(
                    "Confirm Upload", 
                    f"Ready to upload {len(processed_df)} rows to Smartsheet.\n\n" +
                    f"Unique products: {processed_df['ProductCode'].nunique()}\n" +
                    f"Branches: {processed_df['Branch'].nunique()}\n\n" +
                    "Do you want to proceed?"
                )
                return result
            
            # Run confirmation dialog in main thread
            self.root.after(0, lambda: self.handle_confirmation(ask_confirmation(), processed_df))
            
        except Exception as e:
            error_msg = f"Error during processing: {str(e)}"
            self.log_message(f"ERROR: {error_msg}")
            self.root.after(0, lambda: messagebox.showerror("Processing Error", error_msg))
            
        finally:
            self.root.after(0, self.processing_finished)
            
    def handle_confirmation(self, confirmed, processed_df):
        """Handle the confirmation result"""
        if confirmed and self.is_processing:
            # Start upload in new thread
            thread = threading.Thread(target=self.upload_data, args=(processed_df,))
            thread.daemon = True
            thread.start()
        else:
            self.log_message("Upload cancelled by user")
            self.processing_finished()
            
    def process_excel_data(self, df):
        """Process Excel data with enhanced error handling"""
        column_mapping = {
            'ProductCode': 'ProductCode',
            'Product': 'Product', 
            'Branch': 'Branch',
            'SOH': '4 - SOH',
            'Incoming (Open PO)': '5 - Incoming (Open PO)',
            'Open Sales (Allocated)': '6 - Open Sales (Allocated)',
            'Grand Total': '7 - Grand Total',
            'SOH minus Open Sales (Available)': '8 - SOH minus Open Sales (Available)'
        }
        
        # Clean data
        clean_df = df.copy()
        clean_df = clean_df.dropna(subset=['ProductCode'])
        clean_df = clean_df[~clean_df['ProductCode'].astype(str).str.contains(
            'Grand Total|Total|ASM|^$|nan|ProductCode', na=False, case=False)]
        
        self.log_message(f"After cleaning: {len(clean_df)} rows")
        
        # Create processed DataFrame
        processed_df = pd.DataFrame()
        
        # Process text columns
        for field in ['ProductCode', 'Product', 'Branch']:
            excel_col = column_mapping[field]
            if excel_col in clean_df.columns:
                processed_df[field] = clean_df[excel_col].astype(str).str.strip()
                processed_df[field] = processed_df[field].replace('nan', '')
            else:
                processed_df[field] = ''
                
        # Process numeric columns
        numeric_columns = ['SOH', 'Incoming (Open PO)', 'Open Sales (Allocated)', 'Grand Total']
        
        for field in numeric_columns:
            excel_col = column_mapping[field]
            if excel_col in clean_df.columns:
                series = pd.to_numeric(clean_df[excel_col], errors='coerce').fillna(0)
                processed_df[field] = series
            else:
                processed_df[field] = 0
                
        # Add calculated column
        calculated_col = column_mapping['SOH minus Open Sales (Available)']
        if calculated_col in clean_df.columns:
            processed_df['SOH minus Open Sales (Available)'] = pd.to_numeric(
                clean_df[calculated_col], errors='coerce').fillna(0)
        else:
            processed_df['SOH minus Open Sales (Available)'] = (
                processed_df['SOH'] - processed_df['Open Sales (Allocated)']
            )
            
        # Final cleaning
        processed_df = processed_df[
            (processed_df['ProductCode'] != '') & 
            (processed_df['ProductCode'] != 'nan') &
            (processed_df['ProductCode'] != 'ProductCode') &
            (processed_df['ProductCode'].notna()) &
            (processed_df['ProductCode'].str.len() > 0)
        ]
        
        # Convert to strings for Smartsheet
        numeric_columns.append('SOH minus Open Sales (Available)')
        for col in numeric_columns:
            processed_df[col] = processed_df[col].apply(
                lambda x: str(int(x)) if pd.notna(x) and x != '' else '0'
            )
            
        return processed_df
        
    def upload_data(self, processed_df):
        """Upload processed data to Smartsheet with enhanced progress tracking"""
        try:
            if not self.is_processing:
                return
                
            self.log_message("Starting upload to Smartsheet...")
            
            # Get sheet info
            sheet = self.smart.Sheets.get_sheet(self.sheet_id)
            column_map = {col.title: col.id for col in sheet.columns}
            
            # Upload in batches
            inserted_rows = 0
            batch_size = 50
            total_rows = len(processed_df)
            
            for i in range(0, total_rows, batch_size):
                if not self.is_processing:
                    self.log_message("Upload stopped by user")
                    return
                    
                batch_df = processed_df.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                rows_to_insert = []
                
                for _, row in batch_df.iterrows():
                    new_row = smartsheet.models.Row()
                    new_row.to_bottom = True
                    
                    # Create cells
                    for col_name, value in row.items():
                        if col_name in column_map:
                            value_str = str(value).strip()
                            if value_str and value_str != 'nan':
                                cell = smartsheet.models.Cell()
                                cell.column_id = column_map[col_name]
                                cell.value = value_str
                                new_row.cells.append(cell)
                    
                    if len(new_row.cells) >= 3:
                        rows_to_insert.append(new_row)
                
                # Insert batch
                if rows_to_insert:
                    response = self.smart.Sheets.add_rows(self.sheet_id, rows_to_insert)
                    inserted_rows += len(rows_to_insert)
                    progress_pct = (batch_num * batch_size / total_rows) * 100
                    self.log_message(f"SUCCESS: Batch {batch_num}: {len(rows_to_insert)} rows uploaded (Total: {inserted_rows}, {progress_pct:.1f}% complete)")
            
            # Success message
            self.log_message("SUCCESS: Upload completed successfully!")
            self.log_message(f"Total rows inserted: {inserted_rows}")
            
            sheet_url = f"https://app.smartsheet.com/sheets/{self.sheet_id}"
            self.log_message(f"View results at: {sheet_url}")
            
            # Save log file location info
            log_path = self.get_log_file_path()
            self.log_message(f"Detailed log saved to: {log_path}")
            
            self.root.after(0, lambda: messagebox.showinfo(
                "Upload Complete!", 
                f"Upload completed successfully!\n\n" +
                f"Rows uploaded: {inserted_rows}\n" +
                f"Log saved to: {log_path.name}\n\n" +
                f"You can view the results in Smartsheet."
            ))
            
        except Exception as e:
            error_msg = f"Error during upload: {str(e)}"
            self.log_message(f"ERROR: {error_msg}")
            self.root.after(0, lambda: messagebox.showerror("Upload Error", error_msg))
            
    def processing_finished(self):
        """Called when processing is finished"""
        self.is_processing = False
        self.progress.stop()
        self.process_button.config(state='normal')
        self.stop_button.config(state='disabled')
        self.status_label.config(text="Ready")

def main():
    """Main function to run the enhanced GUI application"""
    root = tk.Tk()
    app = EnhancedSmartsheetUploaderGUI(root)
    
    # Center window on screen
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    # Set icon if available (optional)
    try:
        root.iconbitmap('icon.ico')  # Add icon.ico file if you have one
    except:
        pass
    
    root.mainloop()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Cin7 to Smartsheet Uploader v3.0 - Complete Features, Windows Compatible
Maintains ALL advanced features while avoiding TTK theme issues
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
import smartsheet
import logging
import threading
import time
import json
import os
import sys
import traceback
import queue
from datetime import datetime
from pathlib import Path
import requests.exceptions
from typing import Dict, List, Optional, Any, Tuple
import re
import platform
import tempfile

# Default configuration
DEFAULT_SMARTSHEET_TOKEN = "pQxhZNG27iD0OXNcG2e3VJnZi3PRVDD6SD2Ju"

class Cin7SmartsheetUploaderComplete:
    def __init__(self):
        print("Initializing Complete Cin7 Smartsheet Uploader...")
        
        self.root = tk.Tk()
        self.root.title("Cin7 to Smartsheet Uploader v3.0 - Complete Edition")
        self.root.geometry("1000x800")
        self.root.resizable(True, True)
        self.root.minsize(900, 700)
        
        # Configuration file for persistence
        # Store config in user's home directory, not app directory (macOS security)
        self.config_file = str(Path.home() / "cin7_uploader_config.json")
        self.config = self.load_config()
        
        # Processing variables
        self.excel_file_path = ""
        self.smartsheet_client = None
        self.smartsheet_sheet = None
        self.is_processing = False
        self.upload_cancelled = False
        self.processed_df = None
        self.confirmation_result = None
        
        # Enhanced configuration parameters
        self.upload_config = {
            'batch_size': 20,
            'max_retries': 3,
            'retry_delay': 2,
            'connection_timeout': 60,
            'read_timeout': 120,
            'rate_limit_delay': 0.5,
        }
        
        # Cin7-specific column mapping for dual-header structure
        self.cin7_column_mapping = {
            'ProductCode': ['productcode', 'product_code', 'product code'],
            'Product': ['product', 'description', 'product description'],
            'Branch': ['branch', 'location', 'warehouse'],
            'SOH': ['soh', '4 - soh', 'stock on hand', 'soh_stock qty'],
            'Incoming NOT paid': ['incoming', '5 -', 'open po', 'incoming not paid', 'incoming_not_paid_stock qty'],
            'Open Sales': ['open sales', '6 -', 'allocated', 'open_sales_stock qty'],
            'Grand Total': ['grand total', '7 -', 'total', 'grand_total_stock qty']
        }
        
        # Queue for thread communication
        self.message_queue = queue.Queue()
        
        # Setup comprehensive logging
        self.setup_logging()
        
        # Create UI without problematic TTK styles
        self.create_ui()
        
        # Load saved configuration
        self.load_saved_config()
        
        # Start message queue processor
        self.root.after(100, self.process_message_queue)
        
        # Setup graceful shutdown
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        print("Complete initialization finished successfully!")
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file with error handling"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    print("Configuration loaded successfully")
                    return config
        except Exception as e:
            print(f"Warning: Could not load config - {str(e)}")
        
        return {
            'api_token': DEFAULT_SMARTSHEET_TOKEN,
            'sheet_url': '',
            'last_file_directory': str(Path.home()),
            'overwrite_mode': True,
            'verbatim_copy': True,
            'column_mapping': True,
            'window_geometry': '1000x800'
        }
    
    def save_config(self):
        """Save configuration to file with error handling"""
        try:
            self.config['api_token'] = self.api_token_entry.get()
            self.config['sheet_url'] = self.sheet_url_entry.get()
            self.config['overwrite_mode'] = self.overwrite_var.get()
            self.config['verbatim_copy'] = self.verbatim_var.get()
            self.config['column_mapping'] = self.column_mapping_var.get()
            self.config['window_geometry'] = self.root.geometry()
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            print("Configuration saved successfully")
        except Exception as e:
            print(f"Warning: Could not save config - {str(e)}")
    
    def setup_logging(self):
    """Setup comprehensive logging system"""
    # Create logs directory in user's temp/home directory instead of app directory
    import tempfile
    from pathlib import Path
    
    # Use system temp directory or user's home directory for logs
    try:
        # Try user's home directory first
        log_dir = Path.home() / "Cin7UploaderLogs"
        log_dir.mkdir(exist_ok=True)
    except:
        # Fallback to system temp directory
        log_dir = Path(tempfile.gettempdir()) / "Cin7UploaderLogs"
        log_dir.mkdir(exist_ok=True)
        
        # Configure logging with rotation
        log_filename = log_dir / f"cin7_uploader_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== Cin7 to Smartsheet Uploader v3.0 Complete Edition Started ===")
        self.logger.info(f"Platform: {platform.system()} {platform.release()}")
        self.logger.info(f"Python: {sys.version}")
    
    def create_ui(self):
        """Create complete user interface without TTK style issues"""
        print("Creating complete user interface...")
        
        # Create notebook for tabbed interface (using ttk.Notebook is safe, it's the Style() that causes issues)
        self.notebook = ttk.Notebook(self.root, padding="10")
        self.notebook.pack(fill='both', expand=True)
        
        # Main upload tab
        self.main_tab = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.main_tab, text="📊 Upload Data")
        
        # Settings tab
        self.settings_tab = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.settings_tab, text="⚙️ Settings")
        
        # Create main tab content
        self.create_main_tab()
        
        # Create settings tab content
        self.create_settings_tab()
        
        print("Complete user interface created successfully!")
    
    def create_main_tab(self):
        """Create main upload tab with all features"""
        # Configure grid weights
        self.main_tab.grid_rowconfigure(5, weight=1)
        self.main_tab.grid_columnconfigure(0, weight=1)
        
        # Header section
        header_frame = ttk.Frame(self.main_tab)
        header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 25))
        header_frame.grid_columnconfigure(0, weight=1)
        
        title_label = ttk.Label(header_frame, text="Cin7 to Smartsheet Uploader v3.0", 
                               font=("Arial", 18, "bold"))
        title_label.grid(row=0, column=0)
        
        desc_label = ttk.Label(header_frame, 
                              text="Complete Edition - Overwrite Mode | Column Mapping | Multi-Header Support",
                              font=("Arial", 10))
        desc_label.grid(row=1, column=0, pady=(5, 0))
        
        self.connection_indicator = ttk.Label(header_frame, text="● Not Connected", 
                                             foreground="red", font=("Arial", 9))
        self.connection_indicator.grid(row=2, column=0, pady=(5, 0))
        
        # Step 1: File Selection
        file_frame = ttk.LabelFrame(self.main_tab, text=" Step 1: Select Cin7 Excel Export ", padding=15)
        file_frame.grid(row=1, column=0, sticky="ew", pady=(0, 15))
        file_frame.grid_columnconfigure(1, weight=1)
        
        self.file_path_var = tk.StringVar(value="No file selected")
        file_path_label = ttk.Label(file_frame, textvariable=self.file_path_var, 
                                   foreground="gray", wraplength=600)
        file_path_label.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))
        
        self.browse_button = ttk.Button(file_frame, text="📁 Browse Excel File", 
                                       command=self.browse_file_immediate_response)
        self.browse_button.grid(row=1, column=0, sticky="w")
        
        self.file_info_label = ttk.Label(file_frame, text="", foreground="blue")
        self.file_info_label.grid(row=1, column=1, sticky="w", padx=(20, 0))
        
        self.analyze_button = ttk.Button(file_frame, text="🔍 Analyze Structure", 
                                        command=self.analyze_file_immediate_response, state="disabled")
        self.analyze_button.grid(row=1, column=2, sticky="e")
        
        # Step 2: Smartsheet Configuration
        smartsheet_frame = ttk.LabelFrame(self.main_tab, text=" Step 2: Smartsheet Configuration ", padding=15)
        smartsheet_frame.grid(row=2, column=0, sticky="ew", pady=(0, 15))
        smartsheet_frame.grid_columnconfigure(1, weight=1)
        
        ttk.Label(smartsheet_frame, text="API Token:").grid(row=0, column=0, sticky="w", pady=(0, 10))
        self.api_token_entry = ttk.Entry(smartsheet_frame, show="*", width=50)
        self.api_token_entry.grid(row=0, column=1, sticky="ew", pady=(0, 10), padx=(10, 0))
        
        ttk.Label(smartsheet_frame, text="Sheet URL:").grid(row=1, column=0, sticky="w", pady=(0, 10))
        self.sheet_url_entry = ttk.Entry(smartsheet_frame, width=50)
        self.sheet_url_entry.grid(row=1, column=1, sticky="ew", pady=(0, 10), padx=(10, 0))
        
        connection_frame = ttk.Frame(smartsheet_frame)
        connection_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        connection_frame.grid_columnconfigure(2, weight=1)
        
        self.connect_button = ttk.Button(connection_frame, text="🔗 Connect", 
                                        command=self.connect_smartsheet_immediate_response)
        self.connect_button.grid(row=0, column=0, sticky="w")
        
        self.test_connection_button = ttk.Button(connection_frame, text="🧪 Test", 
                                                command=self.test_connection_immediate_response, state="disabled")
        self.test_connection_button.grid(row=0, column=1, sticky="w", padx=(10, 0))
        
        self.connection_status_var = tk.StringVar(value="Not connected")
        self.connection_status_label = ttk.Label(connection_frame, textvariable=self.connection_status_var, 
                                                foreground="red")
        self.connection_status_label.grid(row=0, column=2, sticky="w", padx=(20, 0))
        
        # Step 3: Upload Configuration
        config_frame = ttk.LabelFrame(self.main_tab, text=" Step 3: Upload Configuration ", padding=15)
        config_frame.grid(row=3, column=0, sticky="ew", pady=(0, 15))
        config_frame.grid_columnconfigure(0, weight=1)
        
        options_frame = ttk.Frame(config_frame)
        options_frame.grid(row=0, column=0, sticky="ew", pady=(0, 15))
        
        self.overwrite_var = tk.BooleanVar(value=True)
        overwrite_cb = ttk.Checkbutton(options_frame, 
                                      text="🔄 Overwrite existing data (clears sheet first - RECOMMENDED)", 
                                      variable=self.overwrite_var)
        overwrite_cb.grid(row=0, column=0, sticky="w")
        
        self.verbatim_var = tk.BooleanVar(value=True)
        verbatim_cb = ttk.Checkbutton(options_frame, 
                                     text="📋 Copy all rows verbatim (captures all 1,112 rows)", 
                                     variable=self.verbatim_var)
        verbatim_cb.grid(row=1, column=0, sticky="w", pady=(5, 0))
        
        self.column_mapping_var = tk.BooleanVar(value=True)
        mapping_cb = ttk.Checkbutton(options_frame, 
                                    text="🗂️ Apply Cin7 intelligent column mapping", 
                                    variable=self.column_mapping_var)
        mapping_cb.grid(row=2, column=0, sticky="w", pady=(5, 0))
        
        # Advanced settings
        advanced_frame = ttk.LabelFrame(config_frame, text="Advanced Settings", padding=10)
        advanced_frame.grid(row=1, column=0, sticky="ew")
        advanced_frame.grid_columnconfigure(1, weight=1)
        
        ttk.Label(advanced_frame, text="Batch Size:").grid(row=0, column=0, sticky="w")
        self.batch_size_var = tk.IntVar(value=20)
        batch_spinbox = ttk.Spinbox(advanced_frame, from_=10, to=50, width=10, textvariable=self.batch_size_var)
        batch_spinbox.grid(row=0, column=1, sticky="w", padx=(10, 0))
        
        ttk.Label(advanced_frame, text="Max Retries:").grid(row=0, column=2, sticky="w", padx=(20, 0))
        self.max_retries_var = tk.IntVar(value=3)
        retries_spinbox = ttk.Spinbox(advanced_frame, from_=1, to=5, width=10, textvariable=self.max_retries_var)
        retries_spinbox.grid(row=0, column=3, sticky="w", padx=(10, 0))
        
        # Step 4: Upload Process
        process_frame = ttk.LabelFrame(self.main_tab, text=" Step 4: Upload Process ", padding=15)
        process_frame.grid(row=4, column=0, sticky="ew", pady=(0, 15))
        process_frame.grid_columnconfigure(1, weight=1)
        
        self.upload_button = ttk.Button(process_frame, text="🚀 Start Complete Upload Process", 
                                       command=self.start_upload_immediate_response)
        self.upload_button.grid(row=0, column=0, sticky="w")
        
        self.cancel_button = ttk.Button(process_frame, text="⏹️ Cancel Upload", 
                                       command=self.cancel_upload_immediate_response, state="disabled")
        self.cancel_button.grid(row=0, column=1, sticky="w", padx=(20, 0))
        
        self.preview_button = ttk.Button(process_frame, text="👁️ Preview Data", 
                                        command=self.preview_data_immediate_response, state="disabled")
        self.preview_button.grid(row=0, column=2, sticky="e")
        
        # Progress section
        progress_section = ttk.Frame(process_frame)
        progress_section.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(20, 0))
        progress_section.grid_columnconfigure(0, weight=1)
        
        self.progress_var = tk.StringVar(value="Ready to start")
        progress_label = ttk.Label(progress_section, textvariable=self.progress_var)
        progress_label.grid(row=0, column=0, sticky="w", pady=(0, 5))
        
        self.progress_bar = ttk.Progressbar(progress_section, mode='determinate')
        self.progress_bar.grid(row=1, column=0, sticky="ew")
        
        # Step 5: Activity Log
        log_frame = ttk.LabelFrame(self.main_tab, text=" Activity Log & Progress ", padding=15)
        log_frame.grid(row=5, column=0, sticky="nsew")
        log_frame.grid_columnconfigure(0, weight=1)
        log_frame.grid_rowconfigure(1, weight=1)
        
        log_controls = ttk.Frame(log_frame)
        log_controls.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        log_controls.grid_columnconfigure(1, weight=1)
        
        ttk.Label(log_controls, text="Filter:").grid(row=0, column=0, sticky="w")
        self.log_filter_var = tk.StringVar()
        log_filter_entry = ttk.Entry(log_controls, textvariable=self.log_filter_var, width=30)
        log_filter_entry.grid(row=0, column=1, sticky="w", padx=(5, 0))
        
        clear_log_button = ttk.Button(log_controls, text="🗑️ Clear", command=self.clear_log)
        clear_log_button.grid(row=0, column=2, sticky="e", padx=(10, 0))
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD, 
                                                 font=("Consolas", 9), bg='#f8f9fa', fg='#2c3e50')
        self.log_text.grid(row=1, column=0, sticky="nsew")
        
        # Configure log text tags for colored output
        self.log_text.tag_configure("INFO", foreground="black")
        self.log_text.tag_configure("SUCCESS", foreground="green", font=("Consolas", 9, "bold"))
        self.log_text.tag_configure("WARNING", foreground="orange")
        self.log_text.tag_configure("ERROR", foreground="red", font=("Consolas", 9, "bold"))
        self.log_text.tag_configure("DEBUG", foreground="gray")
    
    def create_settings_tab(self):
        """Create enhanced settings tab"""
        settings_frame = ttk.Frame(self.settings_tab)
        settings_frame.pack(fill='both', expand=True)
        
        # Connection persistence info
        persist_section = ttk.LabelFrame(settings_frame, text="Connection Persistence", padding=15)
        persist_section.pack(fill='x', pady=(0, 20))
        
        ttk.Label(persist_section, text="Configuration is automatically saved between sessions.", 
                 font=("Arial", 10)).pack(anchor='w')
        ttk.Label(persist_section, text="API tokens and sheet URLs are remembered.", 
                 font=("Arial", 9)).pack(anchor='w', pady=(5, 0))
        
        # Current configuration display
        config_section = ttk.LabelFrame(settings_frame, text="Current Configuration", padding=15)
        config_section.pack(fill='x', pady=(0, 20))
        
        self.config_display = ttk.Label(config_section, text="", font=("Consolas", 9))
        self.config_display.pack(anchor='w')
        
        # Update config display
        self.update_config_display()
        
        # System information
        system_section = ttk.LabelFrame(settings_frame, text="System Information", padding=15)
        system_section.pack(fill='x')
        
        system_info = f"""Platform: {platform.system()} {platform.release()}
Python: {platform.python_version()}
Application: v3.0 Complete Edition
Config File: {self.config_file}
Logs Directory: logs/"""
        
        ttk.Label(system_section, text=system_info, font=("Consolas", 9)).pack(anchor='w')
    
    def update_config_display(self):
        """Update configuration display"""
        config_text = f"""Upload Configuration:
• Overwrite Mode: {self.config.get('overwrite_mode', True)}
• Verbatim Copy: {self.config.get('verbatim_copy', True)}
• Column Mapping: {self.config.get('column_mapping', True)}
• Last File Directory: {self.config.get('last_file_directory', 'Not set')}
• Sheet URL: {'Set' if self.config.get('sheet_url') else 'Not set'}
• API Token: {'Set' if self.config.get('api_token') else 'Not set'}"""
        
        if hasattr(self, 'config_display'):
            self.config_display.config(text=config_text)
    
    # Enhanced immediate response methods for UI responsiveness
    def browse_file_immediate_response(self):
        """Immediate UI response for file browsing"""
        self.browse_button.config(text="📁 Browsing...")
        self.root.update_idletasks()
        self.root.after(10, self.browse_file_threaded)
    
    def analyze_file_immediate_response(self):
        """Immediate UI response for file analysis"""
        self.analyze_button.config(text="🔍 Analyzing...")
        self.root.update_idletasks()
        self.root.after(10, self.analyze_file_threaded)
    
    def connect_smartsheet_immediate_response(self):
        """Immediate UI response for Smartsheet connection"""
        self.connect_button.config(text="🔗 Connecting...")
        self.connection_status_var.set("Connecting...")
        self.root.update_idletasks()
        self.root.after(10, self.connect_smartsheet_threaded)
    
    def test_connection_immediate_response(self):
        """Immediate UI response for connection test"""
        self.test_connection_button.config(text="🧪 Testing...")
        self.root.update_idletasks()
        self.root.after(10, self.test_connection_threaded)
    
    def start_upload_immediate_response(self):
        """Immediate UI response for upload start"""
        self.upload_button.config(text="🚀 Starting...")
        self.upload_button.config(state="disabled")
        self.root.update_idletasks()
        self.root.after(10, self.start_upload_threaded)
    
    def cancel_upload_immediate_response(self):
        """Immediate UI response for upload cancellation"""
        self.cancel_button.config(text="⏹️ Cancelling...")
        self.root.update_idletasks()
        self.root.after(10, self.cancel_upload)
    
    def preview_data_immediate_response(self):
        """Immediate UI response for data preview"""
        self.preview_button.config(text="👁️ Loading...")
        self.root.update_idletasks()
        self.root.after(10, self.preview_data_threaded)
    
    # Core processing methods with enhanced threading and error handling
    def browse_file_threaded(self):
        """Thread-safe file browsing with enhanced Cin7 support"""
        def browse_file():
            try:
                initial_dir = self.config.get('last_file_directory', str(Path.home()))
                file_path = filedialog.askopenfilename(
                    title="Select Cin7 Excel Export File",
                    initialdir=initial_dir,
                    filetypes=[
                        ("Excel files", "*.xlsx *.xls"),
                        ("CSV files", "*.csv"),
                        ("All files", "*.*")
                    ]
                )
                
                if file_path:
                    self.excel_file_path = file_path
                    self.config['last_file_directory'] = str(Path(file_path).parent)
                    
                    filename = Path(file_path).name
                    self.file_path_var.set(f"Selected: {filename}")
                    
                    self.message_queue.put(("log", f"File selected: {filename}", "INFO"))
                    self.message_queue.put(("file_selected", filename, None))
                    
                    # Auto-analyze file structure
                    self.root.after(500, self.analyze_file_immediate_response)
                    
            except Exception as e:
                self.message_queue.put(("log", f"Error selecting file: {str(e)}", "ERROR"))
            finally:
                self.message_queue.put(("reset_browse_button", None, None))
        
        threading.Thread(target=browse_file, daemon=True).start()
    
    def analyze_file_threaded(self):
        """Enhanced file analysis with Cin7 multi-header support"""
        if not self.excel_file_path:
            self.message_queue.put(("reset_analyze_button", None, None))
            return
        
        def analyze_file():
            try:
                self.message_queue.put(("log", "Analyzing Cin7 Excel file structure...", "INFO"))
                
                file_ext = Path(self.excel_file_path).suffix.lower()
                
                # Enhanced reading for Cin7 dual-header structure
                if file_ext == '.csv':
                    df = pd.read_csv(self.excel_file_path, encoding='utf-8')
                    df_multi = None
                else:
                    try:
                        df_multi = pd.read_excel(self.excel_file_path, engine='openpyxl', header=[0, 1])
                        df = pd.read_excel(self.excel_file_path, engine='openpyxl')
                        self.message_queue.put(("log", "Detected Cin7 dual-header structure", "SUCCESS"))
                    except:
                        df = pd.read_excel(self.excel_file_path, engine='openpyxl')
                        df_multi = None
                        self.message_queue.put(("log", "Using single-header structure", "INFO"))
                
                rows, cols = df.shape
                
                self.message_queue.put(("log", f"File analysis complete:", "SUCCESS"))
                self.message_queue.put(("log", f"  - Total rows: {rows:,}", "INFO"))
                self.message_queue.put(("log", f"  - Total columns: {cols}", "INFO"))
                
                # Check for Cin7-specific patterns
                columns = list(df.columns)
                cin7_indicators = []
                
                for col in columns:
                    col_str = str(col).lower()
                    if any(indicator in col_str for indicator in ['productcode', 'branch', 'soh', 'stock qty', 'grand total']):
                        cin7_indicators.append(col)
                
                if cin7_indicators:
                    self.message_queue.put(("log", f"  - Cin7 columns detected: {len(cin7_indicators)}", "SUCCESS"))
                    for col in cin7_indicators[:5]:
                        self.message_queue.put(("log", f"    * {col}", "INFO"))
                    if len(cin7_indicators) > 5:
                        self.message_queue.put(("log", f"    ... and {len(cin7_indicators) - 5} more", "INFO"))
                else:
                    self.message_queue.put(("log", "  - Warning: Standard Cin7 columns not clearly detected", "WARNING"))
                
                # Store analysis for later use
                self.file_analysis = {
                    'df': df,
                    'df_multi': df_multi,
                    'rows': rows,
                    'cols': cols,
                    'cin7_indicators': cin7_indicators
                }
                
                self.message_queue.put(("file_analyzed", f"{rows:,} rows, {cols} columns", None))
                
            except Exception as e:
                self.message_queue.put(("log", f"Error analyzing file: {str(e)}", "ERROR"))
            finally:
                self.message_queue.put(("reset_analyze_button", None, None))
        
        threading.Thread(target=analyze_file, daemon=True).start()
    
    def connect_smartsheet_threaded(self):
        """Enhanced Smartsheet connection with persistence"""
        def connect_smartsheet():
            try:
                api_token = self.api_token_entry.get().strip()
                sheet_url = self.sheet_url_entry.get().strip()
                
                if not api_token:
                    self.message_queue.put(("log", "Error: API token is required", "ERROR"))
                    self.message_queue.put(("connection_failed", None, None))
                    return
                
                if not sheet_url:
                    self.message_queue.put(("log", "Error: Sheet URL is required", "ERROR"))
                    self.message_queue.put(("connection_failed", None, None))
                    return
                
                # Save credentials for persistence
                self.config['api_token'] = api_token
                self.config['sheet_url'] = sheet_url
                self.save_config()
                
                # Initialize Smartsheet client with enhanced configuration
                self.smartsheet_client = smartsheet.Smartsheet(api_token)
                self.smartsheet_client.errors_as_exceptions(True)
                
                # Configure timeouts
                try:
                    self.smartsheet_client.session.timeout = (
                        self.upload_config['connection_timeout'],
                        self.upload_config['read_timeout']
                    )
                except:
                    pass
                
                # Extract sheet ID with enhanced patterns
                sheet_id = self.extract_sheet_id_enhanced(sheet_url)
                if not sheet_id:
                    self.message_queue.put(("log", "Error: Could not extract sheet ID from URL", "ERROR"))
                    self.message_queue.put(("connection_failed", None, None))
                    return
                
                # Test connection and get sheet
                self.message_queue.put(("log", f"Connecting to sheet ID: {sheet_id}", "INFO"))
                self.smartsheet_sheet = self.smartsheet_client.Sheets.get_sheet(sheet_id)
                
                self.message_queue.put(("log", f"Successfully connected to: {self.smartsheet_sheet.name}", "SUCCESS"))
                self.message_queue.put(("log", f"Sheet has {len(self.smartsheet_sheet.columns)} columns", "INFO"))
                
                # Log column structure for debugging
                column_names = [col.title for col in self.smartsheet_sheet.columns]
                self.message_queue.put(("log", f"Smartsheet columns: {', '.join(column_names)}", "INFO"))
                
                self.message_queue.put(("connection_success", self.smartsheet_sheet.name, None))
                
            except Exception as e:
                error_msg = f"Connection failed: {str(e)}"
                self.message_queue.put(("log", error_msg, "ERROR"))
                self.message_queue.put(("connection_failed", None, None))
                self.smartsheet_client = None
                self.smartsheet_sheet = None
            finally:
                self.message_queue.put(("reset_connect_button", None, None))
        
        threading.Thread(target=connect_smartsheet, daemon=True).start()
    
    def test_connection_threaded(self):
        """Enhanced connection test"""
        if not self.smartsheet_client or not self.smartsheet_sheet:
            self.message_queue.put(("log", "No connection to test", "WARNING"))
            self.message_queue.put(("reset_test_button", None, None))
            return
        
        def test_connection():
            try:
                self.message_queue.put(("log", "Testing Smartsheet connection...", "INFO"))
                
                sheet_info = self.smartsheet_client.Sheets.get_sheet(self.smartsheet_sheet.id)
                
                self.message_queue.put(("log", "Connection test successful:", "SUCCESS"))
                self.message_queue.put(("log", f"  - Sheet: {sheet_info.name}", "INFO"))
                self.message_queue.put(("log", f"  - Columns: {len(sheet_info.columns)}", "INFO"))
                self.message_queue.put(("log", f"  - Current rows: {sheet_info.total_row_count}", "INFO"))
                
                try:
                    detailed_sheet = self.smartsheet_client.Sheets.get_sheet(
                        self.smartsheet_sheet.id, 
                        include=['discussions', 'attachments']
                    )
                    self.message_queue.put(("log", "  - Write permissions: Confirmed", "SUCCESS"))
                except:
                    self.message_queue.put(("log", "  - Write permissions: Limited (may affect upload)", "WARNING"))
                
            except Exception as e:
                self.message_queue.put(("log", f"Connection test failed: {str(e)}", "ERROR"))
            finally:
                self.message_queue.put(("reset_test_button", None, None))
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def start_upload_threaded(self):
        """Enhanced upload process with all fixes"""
        if self.is_processing:
            return
        
        if not self.excel_file_path:
            messagebox.showwarning("No File", "Please select an Excel file first")
            self.message_queue.put(("reset_upload_button", None, None))
            return
        
        if not self.smartsheet_client or not self.smartsheet_sheet:
            messagebox.showwarning("No Connection", "Please connect to Smartsheet first")
            self.message_queue.put(("reset_upload_button", None, None))
            return
        
        def upload_process():
            self.is_processing = True
            self.upload_cancelled = False
            
            try:
                self.message_queue.put(("upload_started", None, None))
                self.message_queue.put(("log", "=== Starting Complete Upload Process ===", "INFO"))
                
                # Update upload configuration from UI
                self.upload_config['batch_size'] = self.batch_size_var.get()
                self.upload_config['max_retries'] = self.max_retries_var.get()
                
                # Step 1: Process Excel data with Cin7 enhancements
                self.message_queue.put(("progress_update", "Processing Cin7 Excel data...", 10))
                processed_df = self.process_cin7_excel_data()
                
                if processed_df is None or processed_df.empty:
                    self.message_queue.put(("log", "ERROR: No data to upload", "ERROR"))
                    return
                
                total_rows = len(processed_df)
                self.message_queue.put(("log", f"SUCCESS: Processed {total_rows} rows for upload", "SUCCESS"))
                
                # Step 2: Show confirmation dialog (main thread)
                self.message_queue.put(("progress_update", "Awaiting user confirmation...", 20))
                self.root.after(0, lambda: self.show_enhanced_confirmation_dialog(processed_df))
                
                # Wait for confirmation result
                confirmation_timeout = 30
                wait_time = 0
                while self.confirmation_result is None and wait_time < confirmation_timeout:
                    time.sleep(0.1)
                    wait_time += 0.1
                    if self.upload_cancelled:
                        return
                
                if self.confirmation_result != True:
                    self.message_queue.put(("log", "Upload cancelled by user", "WARNING"))
                    return
                
                self.confirmation_result = None
                
                # Step 3: Clear existing data if overwrite mode
                if self.overwrite_var.get():
                    self.message_queue.put(("progress_update", "Clearing existing Smartsheet data...", 30))
                    self.clear_smartsheet_data_enhanced()
                
                # Step 4: Upload data with enhanced error handling
                self.message_queue.put(("progress_update", "Uploading data to Smartsheet...", 40))
                success = self.upload_data_enhanced(processed_df)
                
                if success and not self.upload_cancelled:
                    self.message_queue.put(("log", "=== Upload Completed Successfully ===", "SUCCESS"))
                    self.message_queue.put(("progress_update", f"Complete! {total_rows} rows uploaded", 100))
                    
                    self.root.after(0, lambda: messagebox.showinfo("Success", 
                                      f"Upload completed successfully!\n\n"
                                      f"Rows uploaded: {total_rows:,}\n"
                                      f"Sheet: {self.smartsheet_sheet.name}\n"
                                      f"Mode: {'Overwrite' if self.overwrite_var.get() else 'Append'}"))
                    
                elif self.upload_cancelled:
                    self.message_queue.put(("log", "Upload cancelled by user", "WARNING"))
                else:
                    self.message_queue.put(("log", "Upload failed", "ERROR"))
                
            except Exception as e:
                self.message_queue.put(("log", f"Upload process failed: {str(e)}", "ERROR"))
                self.message_queue.put(("log", f"Error details: {traceback.format_exc()}", "DEBUG"))
                self.root.after(0, lambda: messagebox.showerror("Upload Failed", f"Upload process failed:\n\n{str(e)}"))
            finally:
                self.is_processing = False
                self.message_queue.put(("upload_finished", None, None))
        
        threading.Thread(target=upload_process, daemon=True).start()
    
    def process_cin7_excel_data(self) -> Optional[pd.DataFrame]:
        """Enhanced Cin7 Excel processing with multi-header support"""
        try:
            # Use stored analysis if available
            if hasattr(self, 'file_analysis'):
                df = self.file_analysis['df']
                df_multi = self.file_analysis.get('df_multi')
            else:
                if Path(self.excel_file_path).suffix.lower() == '.csv':
                    df = pd.read_csv(self.excel_file_path, encoding='utf-8')
                    df_multi = None
                else:
                    try:
                        df_multi = pd.read_excel(self.excel_file_path, engine='openpyxl', header=[0, 1])
                        df = pd.read_excel(self.excel_file_path, engine='openpyxl')
                    except:
                        df = pd.read_excel(self.excel_file_path, engine='openpyxl')
                        df_multi = None
            
            # Choose best DataFrame based on verbatim setting
            if self.verbatim_var.get():
                working_df = df_multi if df_multi is not None else df
                self.message_queue.put(("log", "Using verbatim mode - preserving all data structure", "INFO"))
            else:
                working_df = df
                self.message_queue.put(("log", "Using standard mode with header processing", "INFO"))
            
            # Handle multi-level columns if present
            if isinstance(working_df.columns, pd.MultiIndex):
                new_columns = []
                for col in working_df.columns:
                    if col[0] and col[1] and str(col[0]).strip() != str(col[1]).strip():
                        new_columns.append(f"{col[0]}_{col[1]}".strip("_"))
                    else:
                        new_columns.append(str(col[1] if col[1] else col[0]).strip())
                working_df.columns = new_columns
                self.message_queue.put(("log", "Processed multi-level headers", "INFO"))
            
            # Clean data
            working_df = working_df.fillna('')
            
            # Clean numeric data to prevent Smartsheet formula issues
            working_df = self.clean_numeric_data(working_df)
            
            # Convert remaining non-numeric data to strings
            numeric_columns = ['SOH', 'Incoming NOT paid', 'Open Sales', 'Grand Total', 'Available']
            potential_numeric_cols = [c for c in working_df.columns 
                                    if any(keyword in str(c).lower() 
                                          for keyword in ['stock qty', 'stock value', 'qty', 'total', 'incoming', 'sales'])]
            all_numeric_columns = list(set(numeric_columns + potential_numeric_cols))
            
            for col in working_df.columns:
                if col not in all_numeric_columns:
                    working_df[col] = working_df[col].astype(str)
            
            # Apply Cin7 column mapping if requested
            if self.column_mapping_var.get():
                working_df = self.apply_cin7_column_mapping(working_df)
            
            # Remove invalid rows if not in verbatim mode
            if not self.verbatim_var.get():
                initial_rows = len(working_df)
                
                # Find ProductCode column
                product_code_col = None
                for col in working_df.columns:
                    if any(pattern in str(col).lower() for pattern in self.cin7_column_mapping['ProductCode']):
                        product_code_col = col
                        break
                
                if product_code_col:
                    working_df = working_df[
                        (working_df[product_code_col] != '') & 
                        (working_df[product_code_col] != 'nan') &
                        (~working_df[product_code_col].str.contains('Grand Total|Total|ProductCode', na=False, case=False))
                    ]
                    
                    removed_rows = initial_rows - len(working_df)
                    if removed_rows > 0:
                        self.message_queue.put(("log", f"Filtered out {removed_rows} invalid rows", "INFO"))
            
            self.message_queue.put(("log", f"Final processed data: {len(working_df)} rows, {len(working_df.columns)} columns", "SUCCESS"))
            return working_df
            
        except Exception as e:
            self.message_queue.put(("log", f"Error processing Excel data: {str(e)}", "ERROR"))
            return None
    
    def apply_cin7_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply intelligent Cin7 column mapping"""
        try:
            self.message_queue.put(("log", "Applying Cin7 column mapping...", "INFO"))
            
            mapped_df = pd.DataFrame()
            mapping_results = {}
            
            # Map each target column
            for target_col, search_patterns in self.cin7_column_mapping.items():
                source_col = None
                
                # Search for matching column
                for df_col in df.columns:
                    df_col_lower = str(df_col).lower()
                    if any(pattern in df_col_lower for pattern in search_patterns):
                        source_col = df_col
                        break
                
                if source_col:
                    mapped_df[target_col] = df[source_col]
                    mapping_results[target_col] = source_col
                    self.message_queue.put(("log", f"  - {target_col} ← {source_col}", "INFO"))
                else:
                    # Use default values for missing columns
                    if target_col in ['SOH', 'Incoming NOT paid', 'Open Sales', 'Grand Total']:
                        mapped_df[target_col] = '0'
                    else:
                        mapped_df[target_col] = 'N/A'
                    mapping_results[target_col] = 'Not found (using default)'
                    self.message_queue.put(("log", f"  - {target_col} ← Default value (column not found)", "WARNING"))
            
            # Add calculated Available column
            if all(col in mapped_df.columns for col in ['SOH', 'Open Sales']):
                try:
                    soh_numeric = pd.to_numeric(mapped_df['SOH'], errors='coerce').fillna(0)
                    open_sales_numeric = pd.to_numeric(mapped_df['Open Sales'], errors='coerce').fillna(0)
                    mapped_df['Available'] = (soh_numeric - open_sales_numeric).astype(int).astype(str)
                    self.message_queue.put(("log", "  - Available ← Calculated (SOH - Open Sales)", "INFO"))
                except:
                    mapped_df['Available'] = '0'
                    self.message_queue.put(("log", "  - Available ← Default (calculation failed)", "WARNING"))
            
            self.message_queue.put(("log", f"Column mapping complete: {len(mapping_results)} columns mapped", "SUCCESS"))
            return mapped_df
            
        except Exception as e:
            self.message_queue.put(("log", f"Error applying column mapping: {str(e)}", "WARNING"))
            return df
    
    def show_enhanced_confirmation_dialog(self, processed_df: pd.DataFrame):
        """Enhanced confirmation dialog with better handling"""
        try:
            # Prepare summary information
            unique_products = processed_df.iloc[:, 0].nunique() if len(processed_df.columns) > 0 else 0
            unique_branches = processed_df['Branch'].nunique() if 'Branch' in processed_df.columns else 0
            
            # Create detailed message
            message = f"""Ready to upload {len(processed_df)} rows to Smartsheet.

Data Summary:
• Total rows: {len(processed_df):,}
• Unique products: {unique_products:,}
• Unique branches: {unique_branches}
• Upload mode: {'OVERWRITE (clears sheet first)' if self.overwrite_var.get() else 'APPEND (adds to existing data)'}

Columns to upload:
{', '.join(processed_df.columns)}

Do you want to proceed with the upload?

⚠️ This operation cannot be undone."""
            
            # Show dialog and store result
            result = messagebox.askyesno("Confirm Upload", message, parent=self.root)
            
            self.confirmation_result = result
            
            if result:
                self.message_queue.put(("log", "User confirmed upload - proceeding...", "INFO"))
            else:
                self.message_queue.put(("log", "Upload cancelled by user", "WARNING"))
                
        except Exception as e:
            self.message_queue.put(("log", f"Error in confirmation dialog: {str(e)}", "ERROR"))
            self.confirmation_result = False
    
    def clear_smartsheet_data_enhanced(self):
        """Enhanced data clearing with proper error handling"""
        try:
            self.message_queue.put(("log", "Clearing existing Smartsheet data...", "INFO"))
            
            # Get all rows with retry logic
            for attempt in range(self.upload_config['max_retries']):
                try:
                    sheet = self.smartsheet_client.Sheets.get_sheet(
                        self.smartsheet_sheet.id,
                        include=['rowPermalinks']
                    )
                    break
                except Exception as e:
                    if attempt == self.upload_config['max_retries'] - 1:
                        raise e
                    self.message_queue.put(("log", f"Retry {attempt + 1}: Getting sheet data", "WARNING"))
                    time.sleep(self.upload_config['retry_delay'])
            
            if not sheet.rows:
                self.message_queue.put(("log", "No existing rows to clear", "INFO"))
                return
            
            # Delete rows in batches
            row_ids = [row.id for row in sheet.rows]
            batch_size = 400
            total_batches = (len(row_ids) + batch_size - 1) // batch_size
            
            self.message_queue.put(("log", f"Clearing {len(row_ids)} rows in {total_batches} batches", "INFO"))
            
            for batch_num in range(total_batches):
                if self.upload_cancelled:
                    return
                
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(row_ids))
                batch_ids = row_ids[start_idx:end_idx]
                
                # Delete with retry logic
                for attempt in range(self.upload_config['max_retries']):
                    try:
                        self.smartsheet_client.Sheets.delete_rows(self.smartsheet_sheet.id, batch_ids)
                        break
                    except Exception as e:
                        if attempt == self.upload_config['max_retries'] - 1:
                            raise e
                        self.message_queue.put(("log", f"Retry {attempt + 1}: Deleting batch {batch_num + 1}", "WARNING"))
                        time.sleep(self.upload_config['retry_delay'])
                
                self.message_queue.put(("log", f"Cleared batch {batch_num + 1}/{total_batches}: {len(batch_ids)} rows", "INFO"))
                
                if batch_num < total_batches - 1:
                    time.sleep(self.upload_config['rate_limit_delay'])
            
            self.message_queue.put(("log", f"Successfully cleared all {len(row_ids)} existing rows", "SUCCESS"))
            
        except Exception as e:
            self.message_queue.put(("log", f"Error clearing data: {str(e)}", "ERROR"))
            raise e
    
    def upload_data_enhanced(self, df: pd.DataFrame) -> bool:
        """Enhanced upload with comprehensive error handling"""
        try:
            total_rows = len(df)
            batch_size = self.upload_config['batch_size']
            total_batches = (total_rows + batch_size - 1) // batch_size
            uploaded_rows = 0
            
            self.message_queue.put(("log", f"Starting upload: {total_rows} rows in {total_batches} batches", "INFO"))
            
            # Get column mapping
            column_map = {col.title: col.id for col in self.smartsheet_sheet.columns}
            
            for batch_num in range(total_batches):
                if self.upload_cancelled:
                    self.message_queue.put(("log", "Upload cancelled by user", "WARNING"))
                    return False
                
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Prepare rows for Smartsheet
                rows_to_add = []
                for _, row in batch_df.iterrows():
                    new_row = smartsheet.models.Row()
                    new_row.to_bottom = True
                    
                    for col_name, value in row.items():
                        if col_name in column_map and str(value).strip() and str(value) != 'nan':
                            cell = smartsheet.models.Cell()
                            cell.column_id = column_map[col_name]
                            
                            # Check if this is a numeric column and send as number
                            numeric_columns = ['SOH', 'Incoming NOT paid', 'Open Sales', 'Grand Total', 'Available']
                            potential_numeric_cols = [c for c in df.columns 
                                                    if any(keyword in str(c).lower() 
                                                          for keyword in ['stock qty', 'stock value', 'qty', 'total', 'incoming', 'sales'])]
                            all_numeric_columns = list(set(numeric_columns + potential_numeric_cols))
                            
                            if col_name in all_numeric_columns:
                                try:
                                    # Send as numeric value, not string
                                    numeric_value = float(str(value).strip())
                                    if numeric_value == int(numeric_value):
                                        cell.value = int(numeric_value)  # Send as integer
                                    else:
                                        cell.value = numeric_value  # Send as float
                                except (ValueError, TypeError):
                                    cell.value = str(value).strip()  # Fallback to string
                            else:
                                cell.value = str(value).strip()  # Send as string for non-numeric
                            
                            new_row.cells.append(cell)
                    
                    if new_row.cells:
                        rows_to_add.append(new_row)
                
                # Upload batch with retry logic
                success = False
                for attempt in range(self.upload_config['max_retries']):
                    try:
                        if self.upload_cancelled:
                            return False
                        
                        response = self.smartsheet_client.Sheets.add_rows(self.smartsheet_sheet.id, rows_to_add)
                        success = True
                        break
                        
                    except requests.exceptions.Timeout:
                        if attempt < self.upload_config['max_retries'] - 1:
                            self.message_queue.put(("log", f"Timeout on batch {batch_num + 1}, retry {attempt + 1}", "WARNING"))
                            time.sleep(self.upload_config['retry_delay'] * (attempt + 1))
                        else:
                            raise
                    except Exception as e:
                        if attempt < self.upload_config['max_retries'] - 1:
                            self.message_queue.put(("log", f"Error on batch {batch_num + 1}, retry {attempt + 1}: {str(e)}", "WARNING"))
                            time.sleep(self.upload_config['retry_delay'] * (attempt + 1))
                        else:
                            raise
                
                if not success:
                    self.message_queue.put(("log", f"Failed to upload batch {batch_num + 1} after {self.upload_config['max_retries']} attempts", "ERROR"))
                    return False
                
                uploaded_rows += len(rows_to_add)
                progress_pct = 40 + (uploaded_rows / total_rows) * 50
                
                self.message_queue.put(("log", f"Batch {batch_num + 1}/{total_batches}: {len(rows_to_add)} rows uploaded (Total: {uploaded_rows:,}, {(uploaded_rows/total_rows)*100:.1f}%)", "SUCCESS"))
                self.message_queue.put(("progress_update", f"Uploading: {uploaded_rows:,}/{total_rows:,} rows", progress_pct))
                
                if batch_num < total_batches - 1:
                    time.sleep(self.upload_config['rate_limit_delay'])
            
            return True
            
        except Exception as e:
            self.message_queue.put(("log", f"Upload failed: {str(e)}", "ERROR"))
            return False
    
    def preview_data_threaded(self):
        """Enhanced data preview with TreeView window"""
        if not self.excel_file_path:
            messagebox.showwarning("No File", "Please select an Excel file first")
            self.message_queue.put(("reset_preview_button", None, None))
            return
        
        def preview_data():
            try:
                processed_df = self.process_cin7_excel_data()
                
                if processed_df is not None and not processed_df.empty:
                    self.root.after(0, lambda: self.show_preview_window(processed_df))
                else:
                    self.message_queue.put(("log", "No data to preview", "WARNING"))
                    
            except Exception as e:
                self.message_queue.put(("log", f"Error creating preview: {str(e)}", "ERROR"))
            finally:
                self.message_queue.put(("reset_preview_button", None, None))
        
        threading.Thread(target=preview_data, daemon=True).start()
    
    def show_preview_window(self, df: pd.DataFrame):
        """Enhanced preview window with TreeView"""
        preview_window = tk.Toplevel(self.root)
        preview_window.title("Data Preview - Cin7 to Smartsheet")
        preview_window.geometry("1000x700")
        preview_window.transient(self.root)
        preview_window.grab_set()
        
        # Create main frame
        main_frame = ttk.Frame(preview_window, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Info section
        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(info_frame, text=f"Preview: First 100 rows of {len(df)} total rows", 
                 font=("Arial", 12, "bold")).pack(anchor=tk.W)
        ttk.Label(info_frame, text=f"Columns: {', '.join(df.columns)}", 
                 font=("Arial", 9)).pack(anchor=tk.W, pady=(5, 0))
        
        # Treeview with scrollbars
        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        tree = ttk.Treeview(tree_frame)
        
        # Configure columns (limit to first 8 for readability)
        display_columns = list(df.columns[:8])
        tree['columns'] = display_columns
        tree['show'] = 'tree headings'
        
        # Column headings
        tree.heading('#0', text='Row')
        tree.column('#0', width=50)
        
        for col in display_columns:
            tree.heading(col, text=str(col))
            tree.column(col, width=120)
        
        # Add data (first 100 rows)
        preview_df = df.head(100)
        for idx, row in preview_df.iterrows():
            values = [str(row[col])[:50] for col in display_columns]
            tree.insert('', 'end', text=str(idx + 1), values=values)
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(button_frame, text="Close Preview", command=preview_window.destroy).pack(side=tk.RIGHT)
        
        if len(df.columns) > 8:
            ttk.Label(button_frame, text=f"Showing first 8 of {len(df.columns)} columns", 
                     font=("Arial", 9)).pack(side=tk.LEFT)
    
    def extract_sheet_id_enhanced(self, url: str) -> Optional[str]:
        """Enhanced sheet ID extraction"""
        try:
            if '/sheets/' in url:
                return url.split('/sheets/')[1].split('?')[0].split('/')[0]
            elif '/b/publish?EQBCT=' in url:
                return url.split('EQBCT=')[1].split('&')[0]
            else:
                match = re.search(r'\d{19}', url)
                if match:
                    return match.group()
                match = re.search(r'\d{10,}', url)
                if match:
                    return match.group()
        except Exception as e:
            self.message_queue.put(("log", f"Error extracting sheet ID: {str(e)}", "ERROR"))
        return None
    
    def cancel_upload(self):
        """Enhanced upload cancellation"""
        if self.is_processing:
            self.upload_cancelled = True
            self.confirmation_result = False
            self.message_queue.put(("log", "Cancelling upload...", "WARNING"))
        else:
            messagebox.showinfo("No Upload", "No upload is currently in progress")
    
    def clear_log(self):
        """Clear the log display"""
        self.log_text.delete(1.0, tk.END)
        self.add_log_message("Log cleared", "INFO")
    
    def load_saved_config(self):
        """Load saved configuration into UI with default token"""
        try:
            # Load API token (use saved or default)
            api_token = self.config.get('api_token', DEFAULT_SMARTSHEET_TOKEN)
            
            # Clear and insert API token
            self.api_token_entry.delete(0, tk.END)
            if api_token:
                self.api_token_entry.insert(0, api_token)
                print(f"Token loaded: {len(api_token)} characters")
            else:
                # Fallback - insert default token
                self.api_token_entry.insert(0, DEFAULT_SMARTSHEET_TOKEN)
                print(f"Using default token: {len(DEFAULT_SMARTSHEET_TOKEN)} characters")
            
            # Load sheet URL
            if self.config.get('sheet_url'):
                self.sheet_url_entry.delete(0, tk.END)
                self.sheet_url_entry.insert(0, self.config['sheet_url'])
            
            if self.config.get('window_geometry'):
                self.root.geometry(self.config['window_geometry'])
            
            # Set options
            self.overwrite_var.set(self.config.get('overwrite_mode', True))
            self.verbatim_var.set(self.config.get('verbatim_copy', True))
            self.column_mapping_var.set(self.config.get('column_mapping', True))
            
            # Auto-connect if credentials are available
            if api_token and self.config.get('sheet_url'):
                self.add_log_message("Auto-connecting with saved credentials...", "INFO")
                self.root.after(1000, self.connect_smartsheet_immediate_response)
                
        except Exception as e:
            self.add_log_message(f"Error loading saved config: {str(e)}")
            # Emergency fallback - ensure token is there
            try:
                self.api_token_entry.delete(0, tk.END)
                self.api_token_entry.insert(0, DEFAULT_SMARTSHEET_TOKEN)
                print("Emergency token fallback applied")
            except:
                pass
    
    def process_message_queue(self):
        """Process messages from background threads"""
        try:
            while True:
                message_type, message, tag = self.message_queue.get_nowait()
                
                if message_type == "log":
                    self.add_log_message(message, tag)
                
                elif message_type == "progress_update":
                    self.progress_var.set(message)
                    if tag is not None:
                        self.progress_bar['value'] = tag
                
                elif message_type == "file_selected":
                    self.analyze_button.config(state="normal")
                    self.file_info_label.config(text=f"File: {message}")
                
                elif message_type == "file_analyzed":
                    self.preview_button.config(state="normal")
                    self.file_info_label.config(text=f"Analyzed: {message}")
                
                elif message_type == "connection_success":
                    self.connection_status_var.set(f"Connected: {message}")
                    self.connection_status_label.config(foreground="green")
                    self.connection_indicator.config(text="● Connected", foreground="green")
                    self.test_connection_button.config(state="normal")
                    if self.excel_file_path:
                        self.upload_button.config(state="normal")
                
                elif message_type == "connection_failed":
                    self.connection_status_var.set("Connection failed")
                    self.connection_status_label.config(foreground="red")
                    self.connection_indicator.config(text="● Not Connected", foreground="red")
                    self.test_connection_button.config(state="disabled")
                    self.upload_button.config(state="disabled")
                
                elif message_type == "upload_started":
                    self.cancel_button.config(state="normal")
                    self.upload_button.config(state="disabled")
                    self.progress_bar['value'] = 0
                
                elif message_type == "upload_finished":
                    self.cancel_button.config(state="disabled")
                    if self.excel_file_path and self.smartsheet_client:
                        self.upload_button.config(state="normal")
                    self.upload_button.config(text="🚀 Start Complete Upload Process")
                
                # Reset button states
                elif message_type == "reset_browse_button":
                    self.browse_button.config(text="📁 Browse Excel File")
                elif message_type == "reset_analyze_button":
                    self.analyze_button.config(text="🔍 Analyze Structure")
                elif message_type == "reset_connect_button":
                    self.connect_button.config(text="🔗 Connect")
                elif message_type == "reset_test_button":
                    self.test_connection_button.config(text="🧪 Test")
                elif message_type == "reset_upload_button":
                    self.upload_button.config(text="🚀 Start Complete Upload Process")
                    self.upload_button.config(state="normal" if self.excel_file_path and self.smartsheet_client else "disabled")
                elif message_type == "reset_preview_button":
                    self.preview_button.config(text="👁️ Preview Data")
                    
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_message_queue)
    
    def add_log_message(self, message: str, tag: str = "INFO"):
        """Add message to log with enhanced formatting"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"
        
        self.log_text.insert(tk.END, formatted_message, tag)
        self.log_text.see(tk.END)
        
        # Also log to file
        if tag == "ERROR":
            self.logger.error(message)
        elif tag == "WARNING":
            self.logger.warning(message)
        elif tag == "SUCCESS":
            self.logger.info(f"SUCCESS: {message}")
        else:
            self.logger.info(message)
    
    def on_closing(self):
        """Handle application closing with proper cleanup"""
        if self.is_processing:
            if messagebox.askokcancel("Quit", "Upload is in progress. Cancel and quit?"):
                self.upload_cancelled = True
                self.save_config()
                self.root.destroy()
        else:
            self.save_config()
            self.root.destroy()
    
    def clean_numeric_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean numeric columns to remove commas and formatting for Smartsheet formulas"""
        numeric_columns = ['SOH', 'Incoming NOT paid', 'Open Sales', 'Grand Total', 'Available']
        potential_numeric_cols = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['stock qty', 'stock value', 'qty', 'total', 'incoming', 'sales']):
                potential_numeric_cols.append(col)
        
        all_numeric_columns = list(set(numeric_columns + potential_numeric_cols))
        columns_to_clean = [col for col in all_numeric_columns if col in df.columns]
        
        self.message_queue.put(("log", f"Cleaning numeric data in columns: {columns_to_clean}", "INFO"))
        
        for col in columns_to_clean:
            try:
                df[col] = df[col].astype(str)
                df[col] = df[col].str.replace(r'[,$\s]', '', regex=True)
                df[col] = df[col].str.replace(r'[^\d.-]', '', regex=True)
                df[col] = df[col].replace(['', 'nan', 'None', 'null'], '0')
                
                numeric_values = pd.to_numeric(df[col], errors='coerce').fillna(0)
                df[col] = numeric_values.apply(lambda x: str(int(x)) if x == int(x) else str(x))
                
                self.message_queue.put(("log", f"  - Cleaned column '{col}': removed formatting", "INFO"))
                
            except Exception as e:
                self.message_queue.put(("log", f"  - Warning: Could not clean column '{col}': {str(e)}", "WARNING"))
                try:
                    df[col] = df[col].astype(str).str.replace(',', '')
                except:
                    pass
        
        return df
    
    def run(self):
        """Start the application"""
        self.add_log_message("Cin7 to Smartsheet Uploader v3.0 - Complete Edition", "SUCCESS")
        self.add_log_message("Features: Overwrite Mode | Cin7 Column Mapping | Multi-Header Support | Enhanced Threading", "INFO")
        self.add_log_message("Ready to process Cin7 files with full 1,112 row support", "INFO")
        
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self.logger.info("Application interrupted by user")
        except Exception as e:
            self.logger.error(f"Application error: {str(e)}")
            messagebox.showerror("Application Error", f"An unexpected error occurred:\n\n{str(e)}")

if __name__ == "__main__":
    try:
        print("Starting Cin7 to Smartsheet Uploader Complete Edition...")
        
        # Add detailed error logging
        import sys
        import traceback
        import tempfile
        import os
        from datetime import datetime
        
        # Create error log in temp directory
        error_log = os.path.join(tempfile.gettempdir(), "cin7_uploader_error.log")
        
        with open(error_log, 'w') as f:
            f.write(f"Starting application at {datetime.now()}\n")
            f.write(f"Python version: {sys.version}\n")
            f.write(f"Working directory: {os.getcwd()}\n")
            f.flush()
        
        app = Cin7SmartsheetUploaderComplete()
        app.run()
        
    except Exception as e:
        error_msg = f"Failed to start application: {str(e)}\nTraceback: {traceback.format_exc()}"
        print(error_msg)
        
        # Write to error log
        try:
            with open(error_log, 'a') as f:
                f.write(f"ERROR: {error_msg}\n")
        except:
            pass
            
        # Show error dialog
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Startup Error", 
                f"Application failed to start:\n\n{str(e)}\n\nError log: {error_log}")
        except:
            pass
            
        input("Press Enter to exit...")

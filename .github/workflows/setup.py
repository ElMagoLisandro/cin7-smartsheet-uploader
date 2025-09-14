from setuptools import setup

APP = ['cin7_smartsheet_gui.py']
DATA_FILES = []
OPTIONS = {
    'packages': ['pandas', 'smartsheet', 'tkinter', 'openpyxl', 'numpy'],
    'includes': [
        'pandas._libs.tslibs.base',
        'pandas._libs.tslibs.timedeltas',
        'pandas._libs.tslibs.np_datetime',
        'pandas._libs.tslibs.offsets',
        'pandas._libs.tslibs.parsing',
        'pandas._libs.tslibs.period',
        'pandas._libs.tslibs.timestamps',
        'pandas._libs.tslibs.timezones',
        'pandas._libs.tslibs.vectorized',
        'smartsheet.models',
        'smartsheet.util',
        'openpyxl.workbook',
        'openpyxl.worksheet',
        'numpy.core._methods',
        'numpy.lib.format'
    ],
    'excludes': [
        'matplotlib', 
        'scipy', 
        'IPython',
        'jupyter',
        'notebook',
        'pytest',
        'PIL',
        'PyQt5',
        'PyQt6'
    ],
    'resources': [],
    'iconfile': None,  # Add path to .icns file if you have one
    'plist': {
        'CFBundleName': 'Cin7 to Smartsheet Uploader',
        'CFBundleDisplayName': 'Cin7 to Smartsheet Uploader',
        'CFBundleShortVersionString': '2.0.0',
        'CFBundleVersion': '2.0.0',
        'CFBundleIdentifier': 'com.futuratrailers.cin7uploader',
        'CFBundleDocumentTypes': [
            {
                'CFBundleTypeName': 'Excel Files',
                'CFBundleTypeExtensions': ['xlsx', 'xls'],
                'CFBundleTypeRole': 'Viewer'
            }
        ],
        'NSHighResolutionCapable': True,
        'LSMinimumSystemVersion': '10.14.0',
        'NSRequiresAquaSystemAppearance': False
    }
}

setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
    name='Cin7 to Smartsheet Uploader',
    version='2.0.0',
    description='Professional data upload tool for Cin7 inventory management',
    author='Futura Trailers',
    python_requires='>=3.8',
)
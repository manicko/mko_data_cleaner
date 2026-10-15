import platformdirs, os, sys
platformdirs.user_config_dir = lambda *a, **k: sys.argv[1]
import mko_data_cleaner.app as _a
from mko_data_cleaner.app import process_data, initialize_settings
from mko_data_cleaner import ReportResult, SettingsNotInitializedError, __version__
sd = os.path.join(sys.argv[1], 'settings')
print('import OK, __version__ =', __version__, ', settings dir exists:', os.path.isdir(sd))
if os.path.isdir(sd):
    print('FAIL: settings dir created at import')
    sys.exit(1)
sys.exit(0)

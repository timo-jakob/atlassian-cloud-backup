import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
src_path = os.path.join(project_root, 'src')
# Add project root to resolve the 'src' package
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# Add src directory to resolve the 'atlassian_cloud_backup' package
if src_path not in sys.path:
    sys.path.insert(0, src_path)

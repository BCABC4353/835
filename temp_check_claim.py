import importlib.util
from pathlib import Path
from collections import Counter
import dictionary
spec = importlib.util.spec_from_file_location('module835', r'C:\\Users\\Brendan Cameron\\Desktop\\835\\835.py')
module835 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module835)
if not hasattr(module835, 'get_discharge_status_description'):
    module835.get_discharge_status_description = dictionary.get_discharge_status_description
file_path = Path(r'C:\\Users\\Brendan Cameron\\Downloads\\New folder (7)_testing_20251128_141741\\RPA_835_5010_2529644_LAC00001_22070852_2025_04_05_T09_01_25_20250407062248_cEDI.txt')
parsed = module835.parse_835_file(str(file_path))
rows = module835.convert_segments_to_rows(parsed['segments'], parsed['element_delimiter'], str(file_path), parsed['component_delimiter'])
claims = Counter(row['CLM_PatientControlNumber_L2100_CLP'] for row in rows)
print('rows', len(rows))
print('unique claims', len(claims))
print('claim 2458991 count', claims.get('2458991'))

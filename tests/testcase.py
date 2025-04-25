#import etl_pipeline
from etl_pipeline import get_data_from_csv,mail_etl_proc

a=mail_etl_proc()

if a=='Records Inserted Successfully':
    print("testcase pass")
else:
    print("Failed")

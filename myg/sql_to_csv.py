from db_config import DbConfig
import pandas as pd
import datetime

obj = DbConfig()
#
qr = f'SELECT * FROM {obj.data_table}'
obj.cur.execute(qr)
results = obj.cur.fetchall()
df = pd.read_sql(qr, obj.con)
date_today = datetime.datetime.today()
date_today_strf = date_today.strftime("%d_%m_%Y")

output_file = f'{obj.database}_{date_today_strf}.csv'


df.pop('category')
df.pop('zip_code')
df.pop('is_zip')
df.pop('is_login')
df.to_csv(output_file, index=False)

print(f"Data exported and formatted successfully to {output_file}")

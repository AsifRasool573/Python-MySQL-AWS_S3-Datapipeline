#Base liberaries to use

import pandas as pd
import boto3
import mysql.connector
from mysql.connector import Error



# Database Credentials
user='root'
pw='root'
host_name= 'localhost'
db= 'parchposy'


#sets up a session with mysql server.
connection = mysql.connector.connect(
                        host= host_name,
                        user = user,
                        password = pw,
                        database=db)




#### Extracting data from local database



data_query = "Select * from orders"
result = None


cursor = connection.cursor()
try:

    cursor.execute(data_query)
    result = cursor.fetchall()
except Error as err:
    print(f'Error: {err}')
    
    
##### Extracting list of column names of database table.



col_names = """ SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'orders' and Table_schema = 'parchposy'
                    ORDER BY ORDINAL_POSITION """
cursor.execute(col_names)
column_list = cursor.fetchall()
column_names = []
for col in column_list:
        col = col[0]                 #tuple unpacking: as the value is tuple, and it has only 1 element. So, it's easy to unpack and then storing in list
        column_names.append(col)

        
        
### Making DataFrame

order_data = pd.DataFrame(result, columns=column_names)


##### Making connection with AMAZON S3 Bucket and writing file to S3

s3 = boto3.client('s3', aws_access_key_id='{AWS_Access_Key_ID}', aws_secret_access_key = '{AWS_Secret_Access_Key}')
# csv_buf = io.StringIO()
order_data.to_csv(csv_buf,header=True, index=False)
csv_buf.seek(0)
s3.put_object(Bucket='{BUCKET_NAME}', Body=csv_buf.getvalue(), Key='FILE_NAME.csv')

print('The File has been written to S3 Bucket\n\n')



#### Reading an already stored csv file

response = s3.get_object(Bucket='{BUCKET_NAME}', Key='FILE_NAME.csv')
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    test_data = pd.read_csv(response.get("Body"))
    print(test_data)
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")

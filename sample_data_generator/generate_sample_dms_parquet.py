import pandas as pd
import random
from datetime import datetime
from pyarrow.parquet import ParquetWriter

# Hypothetical table schema: id (integer), name (string), and modification_date (datetime)
test_table = pd.DataFrame({
    'id': range(1, 11),
    'name': ['name' + str(i) for i in range(1, 11)],
    'modification_date': [datetime.now()] * 10
})

# Generate random logs
logs = []

# Insert logs
for index, row in test_table.iterrows():
    logs.append({
        'table': 'test_table',
        'op': 'I',
        'timestamp': row['modification_date'],
        'data': row.to_dict()
    })

# Update logs
for _ in range(5):
    index = random.choice(test_table.index)
    test_table.loc[index, 'name'] = 'updated_name' + str(index)
    test_table.loc[index, 'modification_date'] = datetime.now()

    logs.append({
        'table': 'test_table',
        'op': 'U',
        'timestamp': test_table.loc[index, 'modification_date'],
        'data': test_table.loc[index].to_dict()
    })

# Delete logs
for _ in range(3):
    if len(test_table.index) > 0:  # Check if there is any index left
        index = random.choice(test_table.index)
        row = test_table.loc[index].copy()
        test_table = test_table.drop(index)

        logs.append({
            'table': 'test_table',
            'op': 'D',
            'timestamp': datetime.now(),
            'data': row.to_dict()
        })

# Log data
logs_df = pd.DataFrame(logs)

# Save logs to a Parquet file
logs_df.to_parquet('dms_logs.parquet', engine='pyarrow')

print('==================== generated dms logs (saved in dms_logs.parquet) ====================')
print(logs_df)
print('========================================================================================\n\n')


# Display the final state of the table
print('==================== generated dms logs (saved in dms_logs.parquet) ====================')
print(test_table)
print('========================================================================================\n\n')

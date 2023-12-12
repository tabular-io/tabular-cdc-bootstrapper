import pandas as pd
import random
from datetime import datetime

# Initialize table schema: id (integer), name (string), and modification_date (datetime)
test_table = pd.DataFrame({
    'id': range(1, 101),
    'name': ['name' + str(i) for i in range(1, 101)],
    'modification_date': [datetime.now().isoformat()] * 100
})

for i in range(5):
  # Generate random logs
  logs = []

  if i==0:
    # Insert logs
    for index, row in test_table.iterrows():
        log = row.to_dict()
        log.update({
            'table': 'test_table',
            'op': 'I',
            'timestamp': datetime.now().isoformat(),  # Update timestamp
        })
        logs.append(log)

  # Update logs
  for _ in range(5):
    if len(test_table.index) > 0:
      index = random.choice(test_table.index)
      test_table.loc[index, 'name'] = 'updated_name' + str(index)
      test_table.loc[index, 'modification_date'] = datetime.now().isoformat()

      log = test_table.loc[index].to_dict()
      log.update({
          'table': 'test_table',
          'op': 'U',
          'timestamp': datetime.now().isoformat(),  # Update timestamp
      })
      logs.append(log)

  # Delete logs
  for _ in range(3):
      if len(test_table.index) > 0:
          index = random.choice(test_table.index)
          row = test_table.loc[index].copy()
          test_table = test_table.drop(index)

          log = row.to_dict()
          log.update({
              'table': 'test_table',
              'op': 'D',
              'timestamp': datetime.now().isoformat()  # Ensure timestamp captured for delete operation
          })
          logs.append(log)

  # Log data
  logs_df = pd.DataFrame(logs)

  # Save logs to a Parquet file
  logs_df.to_parquet(f'dms_logs_{i+1}.parquet', engine='pyarrow')

  print(f'==================== generated dms logs (saved in dms_logs_{i+1}.parquet) ====================')
  print(logs_df)
  print('========================================================================================\n\n')

# Display the final state of the table
print('============================ generated table -- final state ============================')
print(test_table)
print('========================================================================================\n\n')

import pandas as pd
import numpy as np

# Create 100,000 rows of 'noisy' data
rows = 1000000
data = {
    'id': np.arange(rows),
    'val_float': np.random.uniform(0, 1000, size=rows),
    'val_int': np.random.randint(0, 100, size=rows),
    'category': np.random.choice(['Low', 'Medium', 'High', 'Ultra'], rows),
    'label': np.random.choice([0, 1], rows)
}

df = pd.DataFrame(data)
df.to_csv('chunkflow_test_100k.csv', index=False)
print("Dataset 'chunkflow_test_100k.csv' created successfully.")
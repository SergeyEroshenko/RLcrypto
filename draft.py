import pandas as pd
import numpy as np


a = np.array([[1, 2, 3, 3, 4], [2, 5, 4, 7, 8], [0, 0, 0, 0, 0]]).T
df = pd.DataFrame(a, columns=['price', 'timestamp', 'other'])

idx = df.groupby('price')['timestamp'].transform(max) == df['timestamp']
print(df[idx])
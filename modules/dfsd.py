import pandas as pd

data = {
    'Date': ['2023-08-30', '2023-08-31', '2023-09-01'],
    'Open': [100, 102, 105],
    'Close': [105, 103, 106]
}

# Extract the 'Date' column for indexing
index_labels = data.pop('Date')

# Create the DataFrame using the remaining dictionary values and the extracted index
df = pd.DataFrame.from_dict(data, orient='index')
df.columns = index_labels
df = df.T

print(df)
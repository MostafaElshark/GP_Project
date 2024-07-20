import pandas as pd

# Sample data reading (replace 'your_data.csv' with your CSV file's name)
df = pd.read_csv('discloser.csv')

# Columns to aggregate
cols_to_sum = [
    'Joint Working Amount',
    'Donations and Grants to HCOs and Benefits in Kind to HCOs',
    'Sponsorship agreements with HCOs / third parties appointed by HCOs to manage an Event',
    'Registration Fees',
    'Travel & Accommodation',
    'Fees',
    'Related expenses agreed in the fee for service or consultancy contract',
    'Total'
]

# Aggregate columns by 'Practice Name'
aggregated = df.groupby(['Pharma Company Name','PRACTICE_NAME','Year'])[cols_to_sum].sum().reset_index()

# Save the aggregated data (optional)
aggregated.to_csv('aggregated_data.csv', index=False)

print("Data has been aggregated!")

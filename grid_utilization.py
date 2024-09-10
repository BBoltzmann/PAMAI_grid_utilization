import os
import pandas as pd

# File path for the payment behavior CSV
payment_behavior_path = "/Users/cedriccampbell/Downloads/PAMAI/payment-behaviour.csv"

# Directory containing the merged CSV files
directory = '/Users/cedriccampbell/Downloads/PAMAI/'

# List of merged files where forecasted_charge needs to be calculated and additional columns added
files = [
    'merged_watt_hours_PA_005.csv',
    'merged_watt_hours_IY_015.csv',
    'merged_watt_hours_IY-013.csv',
    'merged_watt_hours_LEP-001.csv',
    'merged_watt_hours_LEP-005.csv',
    'merged_watt_hours_PA-002.csv',
    'merged_watt_hours_PA-003.csv',
    'merged_watt_hours_LEP-004.csv'
]

# Function to calculate the Alias Summary (Charges and Consumption Analysis)
def calculate_alias_summary(payment_behavior_file):
    alias_summary = []
    
    # Load the payment behavior CSV
    df = pd.read_csv(payment_behavior_file)
    
    # Strip leading/trailing spaces from column names
    df.columns = df.columns.str.strip()
    
    # Check if 'Alias' and 'Transaction Type' columns exist
    if 'Transaction Type' not in df.columns or 'Alias' not in df.columns:
        print(f"Skipping: 'Transaction Type' or 'Alias' column not found.")
        return None
    
    # Group data by alias and calculate statistics for each alias
    for alias, group in df.groupby('Alias'):
        total_rows = len(group)
        
        # Count the number of payments, charges, and neither
        payment_count = len(group[group['Transaction Type'] == 'PAYMENT'])
        charge_count = len(group[group['Transaction Type'] == 'CHARGE'])
        neither_count = total_rows - payment_count - charge_count
        
        # Count non-null entries in Rate (flat) and Rate (per kWh) columns
        rate_flat_count = group['Rate (flat)'].notnull().sum()
        rate_per_kwh_count = group['Rate (per kWh)'].notnull().sum()
        
        # Total Payment and Charge Amounts
        total_payment_amount = group[group['Transaction Type'] == 'PAYMENT']['Amount'].sum()
        total_charge_amount = group[group['Transaction Type'] == 'CHARGE']['Amount'].sum()

        # Consumption Analysis
        max_daily_consumption = group['Daily Consumption'].max()
        min_daily_consumption = group['Daily Consumption'].min()
        null_daily_consumption_count = group['Daily Consumption'].isnull().sum()
        less_than_50_count = len(group[group['Daily Consumption'] < 50])
        non_null_daily_consumption_count = group['Daily Consumption'].notnull().sum()

        # Validation logic (example: simple check based on arbitrary conditions)
        validation_message = "Validation Passed" if total_payment_amount > 0 and total_charge_amount > 0 else "Validation Failed"

        # Store the alias summary
        alias_summary.append({
            'Alias': alias,
            'Total Rows': total_rows,
            'Payments': payment_count,
            'Charges': charge_count,
            'Neither': neither_count,
            'Rate (flat) Non-Null': rate_flat_count,
            'Rate (per kWh) Non-Null': rate_per_kwh_count,
            'Total Payment Amount': total_payment_amount,
            'Total Charge Amount': total_charge_amount,
            'Max Daily Consumption': max_daily_consumption,
            'Min Daily Consumption': min_daily_consumption,
            'Null Daily Consumption': null_daily_consumption_count,
            'Daily Consumption < 50': less_than_50_count,
            'Non-Null Daily Consumption': non_null_daily_consumption_count,
            'Validation': validation_message
        })
    
    return alias_summary

# Function to add consumption analysis columns
def add_consumption_analysis(df):
    # Calculate max_Wh_at_time for each hour
    df['Time'] = pd.to_datetime(df['Time'], errors='coerce')
    df['Hour'] = df['Time'].dt.hour

    # Calculate max watt_hours for each hour between 07:00 and 19:00
    hourly_max = df.groupby('Hour')['watt_hours'].max().reset_index()

    # Merge the max hourly data back to the original DataFrame to add 'max_Wh_at_time' column
    df = df.merge(hourly_max, on='Hour', how='left', suffixes=('', '_max'))
    df['max_Wh_at_time'] = df['watt_hours_max']

    # Set max_Wh_at_time to 0 if time is not between 07:00 and 19:00
    df['max_Wh_at_time'] = df.apply(lambda row: row['max_Wh_at_time'] if 7 <= row['Hour'] <= 19 else 0, axis=1)

    # Calculate wasted_energy as the difference between max_Wh_at_time and watt_hours
    df['wasted_energy'] = df['max_Wh_at_time'] - df['watt_hours']

    # Set negative wasted_energy values to zero
    df['wasted_energy'] = df['wasted_energy'].apply(lambda x: max(x, 0))

    # Calculate ratio_of_waste as wasted_energy / max_Wh_at_time (only where max_Wh_at_time > 0)
    df['ratio_of_waste'] = df.apply(lambda row: row['wasted_energy'] / row['max_Wh_at_time'] if row['max_Wh_at_time'] > 0 else pd.NA, axis=1)

    # Calculate charge_multiplier
    def calculate_charge_multiplier(row):
        if pd.isna(row['ratio_of_waste']) or pd.isna(row['max_Wh_at_time']):
            return 1
        elif row['ratio_of_waste'] <= 0.2 or row['max_Wh_at_time'] == 0 or row['wasted_energy'] == 0:
            return 1
        else:
            return round(1 + min(1, row['ratio_of_waste']), 2)
    
    df['charge_multiplier'] = df.apply(calculate_charge_multiplier, axis=1)
    
    return df

# Function to process each file, calculate forecasted_charge, and overwrite the file
def process_file(file_name, alias_summary):
    # Get the file path
    file_path = os.path.join(directory, file_name)
    
    # Load the CSV file
    df = pd.read_csv(file_path)
    
    # Strip leading/trailing spaces from column names
    df.columns = df.columns.str.strip()

    # Add the consumption analysis columns to the file
    df = add_consumption_analysis(df)
    
    # Calculate forecasted_charge for each row based on the alias summary
    df['forecasted_charge'] = df.apply(
        lambda row: row['charge_multiplier'] * next((a['Total Charge Amount'] / a['Charges'] for a in alias_summary if a['Alias'] == row['Alias']), 0),
        axis=1
    )
    
    # Overwrite the original CSV file
    df.to_csv(file_path, index=False)
    print(f"File overwritten with forecasted_charge and consumption analysis columns: '{file_name}'.")

# Main execution
# Step 1: Calculate the Alias Summary from the payment-behaviour.csv
alias_summary = calculate_alias_summary(payment_behavior_path)

if alias_summary:
    # Output the alias summary for inspection
    print("Alias Summary:")
    for summary in alias_summary:
        print(summary)
    
    # Step 2: Process each merged CSV file to add forecasted_charge and consumption analysis columns
    for file in files:
        process_file(file, alias_summary)

print("Processing and overwriting complete for all files with forecasted_charge and consumption analysis.")

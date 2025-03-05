import os
import pandas as pd
import glob

def main():
    # Define paths - updated for the new location inside s2-airdrop folder
    sybils_folder = os.path.join('..', 'miner-checker', 'sybils-address-clusters')
    rewards_file = os.path.join('..', 'rewards-contract-checker', 'rewards_claimed_addresses.csv')
    output_file = 'unique_addresses.csv'
    
    # Initialize a set to store unique addresses
    unique_addresses = set()
    
    # Process all CSV files in the sybils-address-clusters folder
    print(f"Processing files in {sybils_folder}...")
    sybil_files = glob.glob(os.path.join(sybils_folder, '*.csv'))
    
    for file_path in sybil_files:
        print(f"Reading {os.path.basename(file_path)}...")
        df = pd.read_csv(file_path, header=None)
        
        # Extract addresses (first column, split by comma if needed)
        if df.shape[1] > 0:
            # If the first column contains comma-separated values
            if df[0].dtype == 'object' and df[0].str.contains(',').any():
                addresses = df[0].str.split(',', expand=True)[0].tolist()
            else:
                addresses = df[0].tolist()
                
            # Add addresses to our set
            unique_addresses.update([addr.lower() for addr in addresses if isinstance(addr, str)])
    
    # Process rewards_claimed_addresses.csv
    print(f"Reading {rewards_file}...")
    if os.path.exists(rewards_file):
        try:
            rewards_df = pd.read_csv(rewards_file)
            if 'Address' in rewards_df.columns:
                rewards_addresses = rewards_df['Address'].tolist()
                unique_addresses.update([addr.lower() for addr in rewards_addresses if isinstance(addr, str)])
            else:
                print(f"Warning: 'Address' column not found in {rewards_file}")
        except Exception as e:
            print(f"Error reading {rewards_file}: {e}")
    else:
        print(f"Warning: {rewards_file} not found")
    
    # Save unique addresses to CSV
    print(f"Found {len(unique_addresses)} unique addresses")
    output_df = pd.DataFrame(list(unique_addresses), columns=['address'])
    output_df.to_csv(output_file, index=False)
    print(f"Saved unique addresses to {output_file}")

if __name__ == "__main__":
    main() 
import boto3
import requests
import json
import csv
import time
import os
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Configuration
S3_BUCKET = 'heurist-adhoc-data-query'
S3_FOLDER = 'season2-miners/'
S3_ADDRESS_FILE = 's2-miner-addresses-2025-03-04T19-50-31-507Z.txt'  # Update with your actual filename
STATS_API_ENDPOINT = 'https://11dugoz7j6.execute-api.us-east-1.amazonaws.com/prod/stats'
OUTPUT_CSV = 'miner_feature_vectors.csv'
MAX_WORKERS = 10  # Number of concurrent API requests
REQUEST_DELAY = 0.2  # Delay between API requests to avoid rate limiting
MAX_ADDRESSES = 100  # Set to a number for testing with fewer addresses

# Season 2 date range
S2_START_SECONDS = 1721347200  # Fri Jul 19 2024 00:00:00 GMT+0000
S2_END_SECONDS = 1737072000    # Fri Jan 17 2025 00:00:00 GMT+0000
S2_START_DATE = datetime.utcfromtimestamp(S2_START_SECONDS).strftime('%Y-%m-%d')
S2_END_DATE = datetime.utcfromtimestamp(S2_END_SECONDS).strftime('%Y-%m-%d')

def is_valid_evm_address(address):
    """Check if the given string is a valid EVM address"""
    if not isinstance(address, str):
        return False
    if not address.startswith('0x'):
        return False
    if len(address) != 42:  # '0x' + 40 hex chars
        return False
    # Check that the string after '0x' contains only valid hex characters
    try:
        int(address[2:], 16)
        return True
    except ValueError:
        return False

def get_miner_addresses():
    """Retrieve the list of miner addresses from S3"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=f"{S3_FOLDER}{S3_ADDRESS_FILE}"
        )
        content = response['Body'].read().decode('utf-8')
        all_addresses = [addr.strip() for addr in content.split('\n') if addr.strip()]
        print(f"Retrieved {len(all_addresses)} miner addresses from S3")
        
        # Filter out invalid addresses
        valid_addresses = [addr for addr in all_addresses if is_valid_evm_address(addr)]
        invalid_count = len(all_addresses) - len(valid_addresses)
        print(f"Filtered out {invalid_count} invalid addresses")
        print(f"Remaining valid addresses: {len(valid_addresses)}")
        
        # Limit the number of addresses for testing if specified
        if MAX_ADDRESSES:
            valid_addresses = valid_addresses[:MAX_ADDRESSES]
            print(f"Limited to {len(valid_addresses)} addresses for testing")
            
        return valid_addresses
    except Exception as e:
        print(f"Error retrieving addresses from S3: {e}")
        return []

def fetch_miner_stats(address):
    """Fetch stats for a specific miner address using the API endpoint"""
    url = f"{STATS_API_ENDPOINT}?minerId={address}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = json.loads(response.text)
            return {
                'address': address,
                'data': data
            }
        else:
            print(f"Error fetching stats for {address}: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception while fetching stats for {address}: {str(e)}")
        return None

def process_miner_stats(miner_stats):
    """
    Process miner stats to create a feature vector with dimensions 2N 
    where N is the number of days with mining activity
    Feature vector: [llama_day1, waifu_day1, llama_day2, waifu_day2, ...]
    Value: 1 if points on that day, 0 if no points
    Only includes days within the Season 2 date range
    """
    if not miner_stats or 'data' not in miner_stats:
        return None
    
    address = miner_stats['address']
    data = miner_stats['data']
    
    # Process S2 rewards data
    if 's2Rewards' in data:
        # Get the list of daily reward data
        daily_rewards = data['s2Rewards']
        
        # Filter rewards to only include S2 date range
        filtered_rewards = [day for day in daily_rewards 
                           if S2_START_DATE <= day['daily_date'] <= S2_END_DATE]
        
        # Sort by date to ensure chronological order
        filtered_rewards.sort(key=lambda x: x['daily_date'])
        
        # Create the feature vector - one entry for llama and one for waifu per day
        features = []
        dates = []
        
        for day_data in filtered_rewards:
            daily_date = day_data['daily_date']
            dates.append(daily_date)
            
            # Check if there are llama points on this day
            has_llama = day_data['llama_points'] > 0
            # Check if there are waifu points on this day
            has_waifu = day_data['waifu_points'] > 0
            
            # Add binary features (0/1)
            features.append(1 if has_llama else 0)
            features.append(1 if has_waifu else 0)
        
        return {
            'address': address,
            'feature_vector': features,
            'dates': dates,
            'days_active': len(dates)
        }
    
    # Alternative approach if s2Rewards isn't available
    elif 'dailyPoints' in data:
        daily_points = data['dailyPoints']
        
        # Filter points to only include S2 date range
        filtered_points = [day for day in daily_points 
                          if S2_START_DATE <= day['daily_date'] <= S2_END_DATE]
        
        # Sort by date
        filtered_points.sort(key=lambda x: x['daily_date'])
        
        # Create the feature vector
        features = []
        dates = []
        
        for day_data in filtered_points:
            daily_date = day_data['daily_date']
            dates.append(daily_date)
            
            # Check for llama and waifu points
            has_llama = ('daily_llama_points' in day_data and 
                         float(day_data['daily_llama_points']) > 0)
            has_waifu = ('daily_waifu_points' in day_data and 
                         float(day_data['daily_waifu_points']) > 0)
            
            # Add binary features
            features.append(1 if has_llama else 0)
            features.append(1 if has_waifu else 0)
        
        return {
            'address': address,
            'feature_vector': features,
            'dates': dates,
            'days_active': len(dates)
        }
    
    return None

def fetch_stats_parallel(addresses):
    """Fetch stats for multiple addresses in parallel"""
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        # Submit all tasks
        for address in addresses:
            futures.append(executor.submit(fetch_miner_stats, address))
            time.sleep(REQUEST_DELAY)  # Prevent API rate limiting
        
        # Process results as they complete with progress bar
        for future in tqdm(futures, desc="Fetching miner stats"):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Error processing result: {str(e)}")
    
    return results

def save_to_csv(processed_data):
    """Save processed feature vectors to CSV file"""
    if not processed_data:
        print("No data to save")
        return
    
    # Find the maximum feature vector length to determine header size
    max_features = max(len(item['feature_vector']) for item in processed_data if item)
    
    with open(OUTPUT_CSV, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Create header row
        header = ['address', 'days_active']
        for i in range(max_features // 2):
            header.extend([f'llama_day{i+1}', f'waifu_day{i+1}'])
        
        writer.writerow(header)
        
        # Write data rows
        for item in processed_data:
            if not item:
                continue
                
            row = [item['address'], item['days_active']]
            
            # Pad feature vector if needed
            feature_vector = item['feature_vector']
            padded_vector = feature_vector + [0] * (max_features - len(feature_vector))
            row.extend(padded_vector)
            
            writer.writerow(row)
    
    print(f"Saved feature vectors for {len(processed_data)} miners to {OUTPUT_CSV}")

def save_to_s3(file_path):
    """Upload the CSV file to S3"""
    s3_client = boto3.client('s3')
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    s3_key = f"{S3_FOLDER}feature_vectors/miner_feature_vectors_{timestamp}.csv"
    
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded feature vectors to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def main():
    # Get miner addresses
    print("Getting miner addresses...")
    addresses = get_miner_addresses()
    
    if not addresses:
        print("No addresses found. Exiting.")
        return
    
    # Fetch stats for each address
    print(f"Fetching stats for {len(addresses)} addresses...")
    stats_data = fetch_stats_parallel(addresses)
    print(f"Successfully fetched stats for {len(stats_data)} miners")
    
    # Process the stats to create feature vectors
    print("Processing stats to create feature vectors...")
    processed_data = []
    for miner_stats in tqdm(stats_data, desc="Processing feature vectors"):
        processed = process_miner_stats(miner_stats)
        if processed:
            processed_data.append(processed)
    
    print(f"Successfully created feature vectors for {len(processed_data)} miners")
    
    # Save to CSV
    print("Saving to CSV...")
    save_to_csv(processed_data)
    
    # Upload to S3
    print("Uploading to S3...")
    save_to_s3(OUTPUT_CSV)
    
    print("Processing complete!")

if __name__ == "__main__":
    main()
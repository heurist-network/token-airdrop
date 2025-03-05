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
import argparse

# Configuration
S3_BUCKET = 'heurist-adhoc-data-query'
S3_FOLDER = 'season2-miners/'
S3_ADDRESS_FILE = 's2-miner-addresses-2025-03-04T19-50-31-507Z.txt'  # Update with your actual filename
STATS_API_ENDPOINT = 'https://11dugoz7j6.execute-api.us-east-1.amazonaws.com/prod/stats'
OUTPUT_CSV = 'miners_complete_data.csv'
MAX_WORKERS = 10  # Number of concurrent API requests
REQUEST_DELAY = 0.2  # Delay between API requests to avoid rate limiting
MAX_ADDRESSES = 0  # Set to a number for testing with fewer addresses, if 0, then all addresses will be processed

# Default configuration
DEFAULT_CONFIG = {
    'input': None,  # No local input file by default
    'output': OUTPUT_CSV,
    's3_input': f"{S3_FOLDER}{S3_ADDRESS_FILE}",
    's3_output': False,  # Don't upload to S3 by default
    'max_miners': MAX_ADDRESSES,  # Process all miners by default
    'workers': MAX_WORKERS,
    'delay': REQUEST_DELAY
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Collect complete miner data from the stats API')
    parser.add_argument('--input', type=str, help='Path to local file with miner addresses (one per line)')
    parser.add_argument('--output', type=str, default=OUTPUT_CSV, help='Path to output CSV file')
    parser.add_argument('--s3-input', type=str, help='S3 key for miner addresses file')
    parser.add_argument('--s3-output', action='store_true', help='Upload result to S3')
    parser.add_argument('--max-miners', type=int, help='Maximum number of miners to process (for testing)')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help='Number of concurrent workers')
    parser.add_argument('--delay', type=float, default=REQUEST_DELAY, help='Delay between API requests')
    return parser.parse_args()

def get_miner_addresses(config):
    """Get miner addresses from either local file or S3"""
    addresses = []
    
    # Try to load from local file if provided
    if config['input'] and os.path.exists(config['input']):
        with open(config['input'], 'r') as f:
            addresses = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(addresses)} addresses from local file: {config['input']}")
    
    # Otherwise try to load from S3
    elif config['s3_input'] or not addresses:
        s3_key = config['s3_input']
        try:
            s3_client = boto3.client('s3')
            response = s3_client.get_object(
                Bucket=S3_BUCKET,
                Key=s3_key
            )
            content = response['Body'].read().decode('utf-8')
            addresses = [addr.strip() for addr in content.split('\n') if addr.strip()]
            print(f"Loaded {len(addresses)} addresses from S3: {s3_key}")
        except Exception as e:
            print(f"Error retrieving addresses from S3: {e}")
    
    # Limit number of addresses if specified
    if config['max_miners'] and config['max_miners'] > 0:
        addresses = addresses[:config['max_miners']]
        print(f"Limited to {len(addresses)} addresses for testing")
    
    return addresses

def fetch_miner_stats(address):
    """Fetch complete stats for a specific miner address using the API endpoint"""
    url = f"{STATS_API_ENDPOINT}?minerId={address}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            try:
                # Handle response based on content type
                if isinstance(response.text, str):
                    data = json.loads(response.text)
                else:
                    data = response.json()
                
                # Add the address to the data for reference
                return {
                    'address': address,
                    'data': data
                }
            except json.JSONDecodeError:
                print(f"Failed to parse JSON for {address}")
                return None
        else:
            print(f"Error fetching stats for {address}: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception while fetching stats for {address}: {str(e)}")
        return None

def fetch_stats_parallel(addresses, max_workers, delay):
    """Fetch stats for multiple addresses concurrently"""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        # Submit all tasks
        for address in addresses:
            futures.append(executor.submit(fetch_miner_stats, address))
            time.sleep(delay)  # Prevent API rate limiting
        
        # Process results as they complete with progress bar
        for future in tqdm(futures, desc="Fetching miner stats", total=len(futures)):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Error processing result: {str(e)}")
    
    return results

def flatten_miner_data(miner_stats):
    """
    Flatten the nested JSON data into a row format suitable for CSV
    with one row per miner (keeping all relevant fields)
    """
    if not miner_stats or 'data' not in miner_stats:
        return None
    
    address = miner_stats['address']
    data = miner_stats['data']
    
    # Start with the base fields
    flattened = {
        'address': address,
        'hardware': data.get('hardware', ''),
        'status': data.get('status', ''),
        'totalImageCount': data.get('totalImageCount', 0),
        'totalTextCount': data.get('totalTextCount', 0),
        'last24HrsImageCount': data.get('last24HrsImageCount', 0),
        'last24HrsTextCount': data.get('last24HrsTextCount', 0),
        'last24HrsAvailability': data.get('last24HrsAvailability', 0),
        'totalLlamaPoints': data.get('totalLlamaPoints', 0),
        'totalWaifuPoints': data.get('totalWaifuPoints', 0),
        's2CurrentEpochLlamaPoints': data.get('s2CurrentEpochLlamaPoints', 0),
        's2CurrentEpochWaifuPoints': data.get('s2CurrentEpochWaifuPoints', 0),
        's2CurrentEpochLlamaRewards': data.get('s2CurrentEpochLlamaRewards', 0),
        's2CurrentEpochWaifuRewards': data.get('s2CurrentEpochWaifuRewards', 0),
    }
    
    # Store the full JSON as a column for later reference
    flattened['raw_data'] = json.dumps(data)
    
    # Extract active days info from rewards data
    s2Rewards = data.get('s2Rewards', [])
    if s2Rewards:
        # Sort by date
        s2Rewards.sort(key=lambda x: x['daily_date'])
        
        # Add number of active days
        flattened['days_active'] = len(s2Rewards)
        
        # Add first and last active day
        if len(s2Rewards) > 0:
            flattened['first_active_day'] = s2Rewards[0]['daily_date']
            flattened['last_active_day'] = s2Rewards[-1]['daily_date']
        
        # Add total rewards
        flattened['total_llama_reward_tokens'] = sum(day.get('llama_reward_tokens', 0) for day in s2Rewards)
        flattened['total_waifu_reward_tokens'] = sum(day.get('waifu_reward_tokens', 0) for day in s2Rewards)
        
        # Count days with llama/waifu points
        flattened['days_with_llama'] = sum(1 for day in s2Rewards if day.get('llama_points', 0) > 0)
        flattened['days_with_waifu'] = sum(1 for day in s2Rewards if day.get('waifu_points', 0) > 0)
        
        # Create a compact representation of active days pattern
        llama_pattern = ''.join(['1' if day.get('llama_points', 0) > 0 else '0' for day in s2Rewards])
        waifu_pattern = ''.join(['1' if day.get('waifu_points', 0) > 0 else '0' for day in s2Rewards])
        flattened['llama_pattern'] = llama_pattern
        flattened['waifu_pattern'] = waifu_pattern
    else:
        flattened['days_active'] = 0
        flattened['days_with_llama'] = 0
        flattened['days_with_waifu'] = 0
    
    return flattened

def save_to_csv(processed_data, output_file):
    """Save processed data to CSV file"""
    if not processed_data:
        print("No data to save")
        return False
    
    # Get all possible columns from all records
    all_columns = set()
    for item in processed_data:
        if item:
            all_columns.update(item.keys())
    
    # Ensure address is the first column
    columns = ['address']
    for col in sorted(all_columns):
        if col != 'address':
            columns.append(col)
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            
            for item in processed_data:
                if item:
                    writer.writerow(item)
        
        print(f"Saved data for {len(processed_data)} miners to {output_file}")
        return True
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False

def upload_to_s3(file_path):
    """Upload the CSV file to S3"""
    s3_client = boto3.client('s3')
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = os.path.basename(file_path)
    base, ext = os.path.splitext(filename)
    s3_key = f"{S3_FOLDER}complete_data/{base}_{timestamp}{ext}"
    
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded complete miner data to s3://{S3_BUCKET}/{s3_key}")
        return s3_key
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None

def main():
    # Check if command line arguments are provided
    if len(os.sys.argv) > 1:
        # Parse command line arguments if provided
        args = parse_arguments()
        config = {
            'input': args.input,
            'output': args.output,
            's3_input': args.s3_input if args.s3_input else DEFAULT_CONFIG['s3_input'],
            's3_output': args.s3_output,
            'max_miners': args.max_miners,
            'workers': args.workers,
            'delay': args.delay
        }
    else:
        # Use default configuration if no arguments provided
        config = DEFAULT_CONFIG
        print("No command line arguments provided. Using default configuration.")
    
    # Get miner addresses
    print("Getting miner addresses...")
    addresses = get_miner_addresses(config)
    
    if not addresses:
        print("No addresses found. Exiting.")
        return
    
    # Set workers and delay from config
    max_workers = config['workers']
    delay = config['delay']
    
    # Fetch stats for each address
    print(f"Fetching stats for {len(addresses)} addresses using {max_workers} workers...")
    stats_data = fetch_stats_parallel(addresses, max_workers, delay)
    print(f"Successfully fetched stats for {len(stats_data)} miners")
    
    # Process the stats to flatten the data
    print("Processing stats to flatten the data...")
    processed_data = []
    for miner_stats in tqdm(stats_data, desc="Flattening miner data"):
        flattened = flatten_miner_data(miner_stats)
        if flattened:
            processed_data.append(flattened)
    
    print(f"Successfully flattened data for {len(processed_data)} miners")
    
    # Save to CSV
    output_file = config['output']
    print(f"Saving to CSV file: {output_file}...")
    success = save_to_csv(processed_data, output_file)
    
    # Upload to S3 if requested
    if success and config['s3_output']:
        print("Uploading to S3...")
        s3_key = upload_to_s3(output_file)
        if s3_key:
            print(f"CSV file uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
    
    print("Processing complete!")

if __name__ == "__main__":
    main()
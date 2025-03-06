#!/usr/bin/env python3
"""
Token Rewards Calculator for S2 Airdrop

This script fetches miner data from the API, calculates total token rewards
(both waifu and llama tokens) for each address, and outputs the results to a CSV file.
"""

import argparse
import json
import requests
import csv
import time
import os
import boto3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Constants
STATS_API_ENDPOINT = "https://11dugoz7j6.execute-api.us-east-1.amazonaws.com/prod/stats"
S3_BUCKET = "heurist-adhoc-data-query"
S3_FOLDER = "season2-miners/"
S3_ADDRESS_FILE = "s2-miner-addresses-2025-03-04T19-50-31-507Z.txt"
DEFAULT_OUTPUT_FILE = f"miner_rewards_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Calculate token rewards for miner addresses')
    parser.add_argument('--address-file', type=str, help='Local file with miner addresses (one per line)')
    parser.add_argument('--s3-key', type=str, help='S3 key for file with miner addresses')
    parser.add_argument('--max-miners', type=int, default=0, help='Maximum number of miners to process (for testing)')
    parser.add_argument('--workers', type=int, default=10, help='Number of worker threads for parallel processing')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between API requests in seconds')
    parser.add_argument('--output', type=str, default=DEFAULT_OUTPUT_FILE, help='Output CSV file path')
    parser.add_argument('--upload-s3', action='store_true', help='Upload results to S3')
    
    return vars(parser.parse_args())

def get_miner_addresses(config):
    """Get list of miner addresses from file or S3"""
    addresses = []
    
    # Load addresses from local file if specified
    if config['address_file'] and os.path.exists(config['address_file']):
        with open(config['address_file'], 'r') as f:
            addresses = [addr.strip() for addr in f.readlines() if addr.strip()]
        print(f"Loaded {len(addresses)} addresses from file: {config['address_file']}")
    
    # Load addresses from S3 if specified or by default
    elif config['s3_key'] or not addresses:
        # Use provided S3 key or default to the predefined path
        s3_key = config['s3_key'] if config['s3_key'] else f"{S3_FOLDER}{S3_ADDRESS_FILE}"
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
            print(f"Error fetching data for {address}: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception when fetching {address}: {e}")
        return None

def fetch_stats_parallel(addresses, max_workers, delay):
    """Fetch stats for multiple addresses in parallel with rate limiting"""
    results = []
    success_count = 0
    failure_count = 0
    
    print(f"Fetching data for {len(addresses)} addresses using {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, result in enumerate(executor.map(fetch_miner_stats, addresses)):
            if result:
                results.append(result)
                success_count += 1
            else:
                failure_count += 1
            
            # Print progress every 10 addresses
            if (i + 1) % 10 == 0 or i + 1 == len(addresses):
                print(f"Progress: {i+1}/{len(addresses)} (success: {success_count}, failed: {failure_count})")
            
            # Add delay to avoid rate limiting
            if delay > 0:
                time.sleep(delay)
    
    print(f"Completed fetching data. Success: {success_count}, Failed: {failure_count}")
    return results

def calculate_token_rewards(miner_stats):
    """Extract and calculate token rewards from miner data"""
    processed_data = []
    
    for miner in miner_stats:
        address = miner['address']
        
        # Initialize reward values
        waifu_reward_tokens = 0
        llama_reward_tokens = 0
        
        # Extract s2Rewards data if available
        if 's2Rewards' in miner['data'] and miner['data']['s2Rewards']:
            for daily_reward in miner['data']['s2Rewards']:
                # Sum up daily rewards
                waifu_reward_tokens += daily_reward.get('waifu_reward_tokens', 0)
                llama_reward_tokens += daily_reward.get('llama_reward_tokens', 0)
        
        # Calculate total rewards
        total_reward_tokens = waifu_reward_tokens + llama_reward_tokens
        
        # Add to processed data
        processed_data.append({
            'address': address,
            'waifu_reward_tokens': waifu_reward_tokens,
            'llama_reward_tokens': llama_reward_tokens,
            'total_reward_tokens': total_reward_tokens
        })
    
    return processed_data

def save_to_csv(processed_data, output_file):
    """Save processed reward data to CSV file"""
    if not processed_data:
        print("No data to save")
        return None
    
    # Create header and sort data by total rewards (descending)
    headers = ['Address', 'S2 waifu_reward_tokens', 'S2 llama_reward_tokens', 'S2 Total Base Tokens']
    sorted_data = sorted(processed_data, key=lambda x: x['total_reward_tokens'], reverse=True)
    
    try:
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            for item in sorted_data:
                writer.writerow([
                    item['address'],
                    item['waifu_reward_tokens'],
                    item['llama_reward_tokens'],
                    item['total_reward_tokens']
                ])
        
        print(f"Saved data to {output_file}")
        return output_file
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return None

def upload_to_s3(file_path):
    """Upload file to S3 bucket"""
    if not file_path or not os.path.exists(file_path):
        print("File not found, cannot upload to S3")
        return
    
    try:
        file_name = os.path.basename(file_path)
        s3_key = f"rewards/{file_name}"
        
        s3_client = boto3.client('s3')
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        
        print(f"Uploaded {file_path} to S3: {S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def main():
    """Main function to run the reward calculation process"""
    # Parse command line arguments
    config = parse_arguments()
    
    # Get miner addresses
    addresses = get_miner_addresses(config)
    if not addresses:
        print("No addresses found. Please provide a valid address file or S3 key.")
        return
    
    # Fetch miner stats
    miner_stats = fetch_stats_parallel(addresses, config['workers'], config['delay'])
    if not miner_stats:
        print("No miner stats retrieved. Exiting.")
        return
    
    # Calculate token rewards
    processed_data = calculate_token_rewards(miner_stats)
    
    # Save to CSV
    output_file = save_to_csv(processed_data, config['output'])
    
    # Upload to S3 if requested
    if config['upload_s3'] and output_file:
        upload_to_s3(output_file)
    
    print("Token rewards calculation completed.")

if __name__ == "__main__":
    main() 
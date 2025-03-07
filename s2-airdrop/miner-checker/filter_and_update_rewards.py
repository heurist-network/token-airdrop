import csv
import json
import re
import os
from datetime import datetime

# Regular expression to match valid Ethereum addresses
ETH_ADDRESS_PATTERN = re.compile(r'^0x[a-fA-F0-9]{40}$')

def is_valid_ethereum_address(address):
    """Check if an address is a valid Ethereum address."""
    return bool(ETH_ADDRESS_PATTERN.match(address))

def load_unique_addresses(file_path):
    """Load unique addresses from the CSV file."""
    unique_addresses = set()
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            unique_addresses.add(row['address'].lower())
    return unique_addresses

def load_miner_stats(file_path):
    """Load miner stats from the JSON file."""
    with open(file_path, 'r') as f:
        stats = json.load(f)
    # Convert to a dictionary for easier lookup
    stats_dict = {item['address'].lower(): item for item in stats}
    return stats_dict

def process_miner_rewards(input_rewards_path, unique_addresses_path, miner_stats_path, output_path):
    """Process miner rewards according to the requirements."""
    # Load unique addresses to be filtered out
    unique_addresses = load_unique_addresses(unique_addresses_path)
    
    # Load miner stats for comparison
    miner_stats = load_miner_stats(miner_stats_path)
    
    # List to store processed data
    processed_data = []
    
    # Initialize totals for tracking
    total_waifu_rewards = 0.0
    total_llama_rewards = 0.0
    total_base_tokens = 0.0
    
    # Process the rewards file
    with open(input_rewards_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            address = row['Address'].lower()
            
            # Step 1: Filter out non-EVM addresses
            if not is_valid_ethereum_address(address):
                continue
                
            # Step 2: Filter out addresses that overlap with unique_addresses.csv
            if address in unique_addresses:
                continue
                
            # Step 3: Compare with miner stats and update if necessary
            if address in miner_stats:
                stats = miner_stats[address]
                
                # Get the values from both sources
                csv_total_tokens = float(row['S2 Total Base Tokens'])
                json_total_tokens = float(stats['revisedTokens']) if 'revisedTokens' in stats else float(stats['totalTokens'])
                
                # If JSON value is larger, use CSV value (keep as is)
                if json_total_tokens >= csv_total_tokens:
                    processed_data.append(row)
                    total_waifu_rewards += float(row['S2 waifu_reward_tokens'])
                    total_llama_rewards += float(row['S2 llama_reward_tokens'])
                    total_base_tokens += csv_total_tokens
                else:
                    # Otherwise, update with JSON values
                    updated_row = row.copy()
                    # updated_row['S2 waifu_reward_tokens'] = str(stats['totalWaifu'])
                    # updated_row['S2 llama_reward_tokens'] = str(stats['totalLlama'])
                    updated_row['S2 Total Base Tokens'] = str(json_total_tokens)
                    processed_data.append(updated_row)
                    total_waifu_rewards += float(row['S2 waifu_reward_tokens'])
                    total_llama_rewards += float(row['S2 llama_reward_tokens'])
                    total_base_tokens += json_total_tokens
            else:
                # Address not in miner stats, keep original values
                processed_data.append(row)
                total_waifu_rewards += float(row['S2 waifu_reward_tokens'])
                total_llama_rewards += float(row['S2 llama_reward_tokens'])
                total_base_tokens += float(row['S2 Total Base Tokens'])
    
    # Step 4: Filter out addresses with less than 1 token reward
    filtered_data = [row for row in processed_data if float(row['S2 Total Base Tokens']) >= 1]
    
    # Recalculate totals after filtering
    total_waifu_rewards = sum(float(row['S2 waifu_reward_tokens']) for row in filtered_data)
    total_llama_rewards = sum(float(row['S2 llama_reward_tokens']) for row in filtered_data)
    total_base_tokens = sum(float(row['S2 Total Base Tokens']) for row in filtered_data)
    
    # Output the final results
    with open(output_path, 'w', newline='') as f:
        if filtered_data:
            fieldnames = filtered_data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(filtered_data)
            
            # Add a blank row for better readability
            blank_row = {field: "" for field in fieldnames}
            writer.writerow(blank_row)
            
            # Add total row
            total_row = {
                'Address': "TOTAL",
                'S2 waifu_reward_tokens': str(total_waifu_rewards),
                'S2 llama_reward_tokens': str(total_llama_rewards),
                'S2 Total Base Tokens': str(total_base_tokens)
            }
            writer.writerow(total_row)
            
            print(f"Processed data written to {output_path}")
            print(f"Total S2 waifu rewards: {total_waifu_rewards}")
            print(f"Total S2 llama rewards: {total_llama_rewards}")
            print(f"Total S2 base tokens: {total_base_tokens}")
        else:
            print("No data to write.")

if __name__ == "__main__":
    # File paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(current_dir)
    
    input_rewards_path = os.path.join(current_dir, "miner_rewards_20250306_164946.csv")
    unique_addresses_path = os.path.join(base_dir, "unique-addresses-collector", "unique_addresses.csv")
    miner_stats_path = os.path.join(current_dir, "top-miner-stats-0114-revise.json")
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(current_dir, f"filtered_miner_rewards_{timestamp}.csv")
    
    # Process the data
    process_miner_rewards(input_rewards_path, unique_addresses_path, miner_stats_path, output_path) 
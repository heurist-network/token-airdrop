# Unique Addresses Collector

This tool collects unique Ethereum addresses from:
1. All CSV files in the `s2-airdrop/miner-checker/sybils-address-clusters/` folder
2. The `s2-airdrop/rewards-contract-checker/rewards_claimed_addresses.csv` file

## Requirements
- Python 3.6+
- pandas library

## Installation
```bash
pip install pandas
```

## Usage
Run the script from within the `unique-addresses-collector` folder:
```bash
python collect_unique_addresses.py
```

## Output
The script generates a file called `unique_addresses.csv` containing all unique Ethereum addresses found across the input files. All addresses are converted to lowercase for consistency. 
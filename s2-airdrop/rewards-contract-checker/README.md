# Rewards Contract Checker

This tool collects addresses that have claimed rewards from the rewards contract `0xCf84821b828Fb21f531A02DD5f30fb029757E30C` on Base chain by monitoring the `RewardsClaimed` events.

## Requirements

- Node.js (v16 or higher recommended)
- npm or yarn

## Installation

1. Clone this repository or navigate to the directory
2. Install dependencies:

```bash
npm install
# or
yarn install
```

## Usage

Run the script:

```bash
npm start
# or
yarn start
```

The script will:
1. Connect to the Base blockchain
2. Collect all addresses from `RewardsClaimed` events between blocks 26735880 and 27152990
3. Output the unique addresses to a CSV file named `rewards_claimed_addresses.csv`

## Configuration

You can modify the following variables in the `index.ts` file:

- `REWARDS_CONTRACT_ADDRESS`: The address of the rewards contract
- `START_BLOCK`: The starting block number for the event search
- `END_BLOCK`: The ending block number for the event search
- `BATCH_SIZE`: Number of blocks to process in a single batch (to avoid timeouts)
- `OUTPUT_FILE`: The name of the output CSV file
- `BASE_RPC_URL`: The RPC URL for the Base chain

## Notes

- The script processes blocks in batches to avoid RPC timeouts.
- Only unique addresses are saved to the CSV file.
- The script handles errors gracefully and will continue processing other batches if one fails. 
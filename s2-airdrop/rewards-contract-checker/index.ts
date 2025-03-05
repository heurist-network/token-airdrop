import { ethers } from 'ethers';
import * as fs from 'fs';
import { createObjectCsvWriter } from 'csv-writer';

// Configuration
const REWARDS_CONTRACT_ADDRESS = '0xCf84821b828Fb21f531A02DD5f30fb029757E30C';
const START_BLOCK = 26735880;
const END_BLOCK = 27152990;
const BATCH_SIZE = 10000; // Number of blocks per batch to avoid RPC timeouts
const OUTPUT_FILE = 'rewards_claimed_addresses.csv';
const BASE_RPC_URL = 'https://mainnet.base.org'; // Base Chain RPC URL

// ABI fragment for the RewardsClaimed event
const eventFragment = [
  'event RewardsClaimed(address indexed rewardee, uint256 totalRewards, uint8 rewardType)'
];

async function main() {
  console.log('Starting to collect addresses from RewardsClaimed events...');

  // Connect to the Base Chain
  const provider = new ethers.JsonRpcProvider(BASE_RPC_URL);
  console.log('Connected to Base Chain');

  // Create a contract instance with only the event ABI we need
  const contract = new ethers.Contract(REWARDS_CONTRACT_ADDRESS, eventFragment, provider);

  // Set up a filter for the RewardsClaimed event
  const filter = contract.filters.RewardsClaimed();
  
  // Set to collect unique addresses
  const uniqueAddresses = new Set<string>();
  
  // Process in batches to avoid timeouts
  for (let fromBlock = START_BLOCK; fromBlock <= END_BLOCK; fromBlock += BATCH_SIZE) {
    const toBlock = Math.min(fromBlock + BATCH_SIZE - 1, END_BLOCK);
    console.log(`Fetching events from blocks ${fromBlock} to ${toBlock}...`);
    
    try {
      const events = await contract.queryFilter(filter, fromBlock, toBlock);
      console.log(`Found ${events.length} events in this batch`);
      
      // Extract addresses from events
      for (const event of events) {
        // In ethers v6, we need to check if the event is an EventLog which has the args property
        if ('args' in event && event.args) {
          // The first argument is the rewardee address
          const rewardee = event.args[0];
          if (rewardee) {
            uniqueAddresses.add(rewardee.toLowerCase());
          }
        } else {
          console.warn('Event without args property encountered');
        }
      }
    } catch (error) {
      console.error(`Error fetching events in batch ${fromBlock}-${toBlock}:`, error);
    }
  }

  // Convert set to array for CSV writing
  const addressesArray = Array.from(uniqueAddresses);
  console.log(`Total unique addresses collected: ${addressesArray.length}`);

  // Write addresses to CSV file
  const csvWriter = createObjectCsvWriter({
    path: OUTPUT_FILE,
    header: [
      { id: 'address', title: 'Address' }
    ]
  });

  const records = addressesArray.map(address => ({ address }));
  await csvWriter.writeRecords(records);

  console.log(`Addresses saved to ${OUTPUT_FILE}`);
}

main().catch(error => {
  console.error('Error in main function:', error);
  process.exit(1);
}); 
# rewards contract address collectors 

## Requirements  

collect the address in the event emitted `RewardsClaimed (index_topic_1 address rewardee, uint256 totalRewards, uint8 rewardType)`  by the rewards contract `0xCf84821b828Fb21f531A02DD5f30fb029757E30C`  on base chain, where the address is in the "rewardee" field, that with starting block number 26735880 and ending block number 27152990.

## Languages 
Using typescript and ethers.js (v6) to interact with the base chain and get the event data. 

## results
Output the collected addresses to a csv file named "rewards_claimed_addresses.csv"


## Cluster analysis on google colab 
by uploading the miner_feature_vectors.csv file to the colab, we can perform cluster analysis on the miner features.

link: https://colab.research.google.com/drive/1S9HOJxeIz6F-b1nTPX7DZzeljZIEZkle?usp=sharing

## Get S2 miner addresses
use `heurist-adhoc-data-query` lambda function to get the s-2 miner addresses and stored in the s3 bucket 

```
import pg from 'pg';
const { Client } = pg;
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

// Lambda that can be used for ad hoc data queries and with options to write to S3

const s3Client = new S3Client({ region: 'us-east-1' });

// S3 configuration
const S3_BUCKET = 'heurist-adhoc-data-query';
const S3_FOLDER = 'season2-miners/'; // Dedicated folder for S2 miners

// Date constants for Season 2
const S2_START_SECONDS = 1721347200; // Fri Jul 19 2024 00:00:00 GMT+0000
const S2_END_SECONDS = 1737072000; // Fri Jan 17 2025 00:00:00 GMT+0000
const S2_START_DATE = new Date(S2_START_SECONDS * 1000).toISOString();
const S2_END_DATE = new Date(S2_END_SECONDS * 1000).toISOString();

async function fetchUniqueMiners(pgClient) {
    // Simple query to just get the unique miner_id values
    const query = `
        SELECT DISTINCT miner_id
        FROM miner_performance
        WHERE hourly_time >= $1 AND hourly_time <= $2
        ORDER BY miner_id;
    `;

    console.log(`Executing query for unique miners active between ${S2_START_DATE} and ${S2_END_DATE}`);
    const result = await pgClient.query(query, [S2_START_DATE, S2_END_DATE]);
    console.log(`Query returned ${result.rows.length} unique miners`);

    if (result.rows.length > 0) {
        // Extract just the miner_id values
        const minerAddresses = result.rows.map(row => row.miner_id);
        return minerAddresses;
    } else {
        console.log('No miners found.');
        return [];
    }
}

async function uploadAddressesToS3(minerAddresses) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `s2-miner-addresses-${timestamp}.txt`;
    const key = `${S3_FOLDER}${filename}`; // Include the folder in the key
    
    // Create a text file with one address per line
    const addressData = minerAddresses.join('\n');
    
    const buffer = Buffer.from(addressData);
    const command = new PutObjectCommand({
        Bucket: S3_BUCKET,
        Key: key,
        Body: buffer,
        ContentType: 'text/plain'
    });
    
    const result = await s3Client.send(command);
    console.log(`Successfully uploaded ${minerAddresses.length} miner addresses to s3://${S3_BUCKET}/${key}`);
    
    return {
        bucket: S3_BUCKET,
        key: key,
        folder: S3_FOLDER,
        filename: filename,
        addressCount: minerAddresses.length,
        url: `https://s3.amazonaws.com/${S3_BUCKET}/${key}`
    };
}

export const handler = async (event) => {
    // Extract custom folder from event if provided
    let folderPath = S3_FOLDER;
    if (event && event.folder) {
        folderPath = event.folder.endsWith('/') ? event.folder : `${event.folder}/`;
        console.log(`Using custom folder path: ${folderPath}`);
    }
    
    const pgClient = new Client({
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME,
        ssl: {
            rejectUnauthorized: false
        }
    });
    
    try {
        console.log("Connecting to PostgreSQL database...");
        await pgClient.connect();
        console.log("Connected successfully");
        
        console.log("Fetching unique miner addresses...");
        const minerAddresses = await fetchUniqueMiners(pgClient);
        
        let uploadResult = null;
        if (minerAddresses.length > 0) {
            console.log(`Uploading ${minerAddresses.length} miner addresses to S3 folder: ${folderPath}...`);
            uploadResult = await uploadAddressesToS3(minerAddresses);
            console.log("Upload completed successfully");
        } else {
            console.log("No miner addresses to upload");
        }
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'S2 miner addresses extraction completed successfully',
                minerCount: minerAddresses.length,
                s3Location: uploadResult
            })
        };
    } catch (error) {
        console.error("Error in Lambda execution:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'Error extracting S2 miner addresses',
                error: error.message
            })
        };
    } finally {
        try {
            await pgClient.end();
            console.log("Database connection closed");
        } catch (err) {
            console.error("Error closing database connection:", err);
        }
    }
};
```
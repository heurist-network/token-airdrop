import pandas as pd

def filter_airdrop_data(input_file, output_file):
    df = pd.read_csv(input_file)
    print(f"Original number of rows: {len(df):,}")
    
    # Get only token columns (excluding Address and Points columns)
    token_columns = [col for col in df.columns if col not in ['Address', 'S1 Llama Points', 'S1 Waifu Points']]
    filter_condition_1 = df[token_columns].any(axis=1)
    
    s1_columns = ['S1 Total Base Tokens', 'S1 Total Bonus Tokens']
    filter_condition_2 = df[s1_columns].any(axis=1)
    
    final_filter = filter_condition_1 & filter_condition_2
    filtered_df = df[final_filter]
    
    rows_removed = len(df) - len(filtered_df)
    print(f"Rows removed: {rows_removed:,}")
    print(f"Remaining rows: {len(filtered_df):,}")
    
    filtered_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    filter_airdrop_data("s1_results.csv", "s1_results_filtered.csv")
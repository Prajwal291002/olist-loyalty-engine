import pandas as pd
import requests
import time
import json
import math

# --- CONFIGURATION ---
HUBSPOT_ACCESS_TOKEN = 'Access Token Here'  # Replace this!
BATCH_SIZE = 10  # Safe limit for free tier (can go up to 100, but 10 is safer)
API_URL = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"

# --- HEADERS ---
headers = {
    'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

def prepare_data_for_hubspot():
    print(" Loading Final Campaign List...")
    df = pd.read_csv('../data/processed/final_campaign_list.csv')
    
    # 1. GENERATE FAKE EMAILS (Required for HubSpot API uniqueness)
    # We use the unique ID to create a consistent fake email
    df['email'] = df['customer_unique_id'] + "@olist.placeholder.com"
    
    # 2. MARK AS NON-MARKETING (To save money)
    # df['hs_marketable_status'] = 'false'
    
    # 3. RENAME COLUMNS to match HubSpot Internal Names
    # You must have created 'olist_user_id' and 'loyalty_tier' in HubSpot first!
    # Mapping: DataFrame Column -> HubSpot Internal Property Name
    records = []
    for index, row in df.iterrows():
        properties = {
            "email": row['email'],
            "firstname": "Olist User", # Placeholder
            "lastname": row['customer_unique_id'][:8], # First 8 chars of ID
            "olist_user_id": row['customer_unique_id'],
            "loyalty_tier": row['Tier'],
            "marketing_action": row['Marketing_Action']
    

            
        }
        records.append({"properties": properties})
    
    return records

def send_to_hubspot(records):
    total_records = len(records)
    num_batches = math.ceil(total_records / BATCH_SIZE)
    
    print(f" Starting Upload: {total_records} contacts in {num_batches} batches...")
    
    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        batch = records[start_idx:end_idx]
        
        payload = json.dumps({"inputs": batch})
        
        try:
            response = requests.post(API_URL, data=payload, headers=headers)
            
            # RATE LIMIT HANDLING (429 Error)
            if response.status_code == 429:
                print(" Rate Limit Hit. Sleeping for 10 seconds...")
                time.sleep(10)
                # Retry logic would go here in production
                continue
                
            if response.status_code not in [200, 201]:
                print(f" Error Batch {i}: {response.text}")
            else:
                # Print progress every 10 batches to avoid spam
                if i % 10 == 0:
                    print(f" Batch {i}/{num_batches} uploaded successfully.")
                    
            # FREE TIER SAFETY PAUSE
            # HubSpot Free allows ~100 requests/10sec. 
            # We sleep slightly to be safe.
            time.sleep(0.5) 
            
        except Exception as e:
            print(f" Critical connection error: {e}")
            break



# TEST LOGIC

# if __name__ == "__main__":
#     # 1. Prepare
#     contact_records = prepare_data_for_hubspot()
    
#     # 2. Test with just the first 5 records (Safety Check)
#     print("\n TEST MODE: Uploading first 5 records only.")
#     send_to_hubspot(contact_records[:5])
    
#     # User prompt to continue
#     # confirm = input("Check HubSpot. Did the 5 records appear? (yes/no): ")
#     # if confirm.lower() == 'yes':
#     #     send_to_hubspot(contact_records[5:])

if __name__ == "__main__":
    # 1. Prepare
    contact_records = prepare_data_for_hubspot()
    
    # --- PRODUCTION MODE ---
    print(f"\n PRODUCTION MODE: Uploading ALL {len(contact_records)} records.")
    
    # Double check before flying
    confirm = input("Are you sure you want to upload 96k contacts? (yes/no): ")
    if confirm.lower() == 'yes':
        send_to_hubspot(contact_records)
    else:
        print("❌ Upload cancelled.")
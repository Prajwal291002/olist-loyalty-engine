import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from a .env file located in the root directory
load_dotenv()

def get_db_connection():
    """
    Creates and returns a SQLAlchemy engine connected to PostgreSQL.
    Pulls credentials from environment variables.
    """
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5433")
    db_name = os.getenv("DB_NAME", "biba")
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASS")

    # Create the connection string
    conn_str = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(conn_str)

def load_and_clean_data():
    """
    Loads Olist datasets via PostgreSQL, cleans dates, filters ghost orders,
    and merges them into a single 'Golden Record' dataframe.
    """
    print("🚀 Starting Data Ingestion from PostgreSQL...")

    engine = get_db_connection()

    # 1. Load Data from Database
    # NOTE: Ensure these table names match exactly how you named them in PostgreSQL
    print("📥 Fetching tables from database...")
    orders = pd.read_sql("SELECT * FROM olist_orders_dataset", engine)
    items = pd.read_sql("SELECT * FROM olist_order_items_dataset", engine)
    customers = pd.read_sql("SELECT * FROM olist_customers_dataset", engine)
    reviews = pd.read_sql("SELECT * FROM olist_order_reviews_dataset", engine)
    products = pd.read_sql("SELECT * FROM olist_products_dataset", engine)

    # 2. Fix Date Types
    print("🕒 Timeline fixing...")
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    
    for col in date_cols:
        orders[col] = pd.to_datetime(orders[col], errors='coerce')

    # 3. Filter "Ghost Orders" (Undelivered)
    initial_count = len(orders)
    orders = orders[orders['order_status'] == 'delivered']
    print(f"👻 Filtered out {initial_count - len(orders)} non-delivered orders.")

    # 4. Enrich Items with Category
    items_enriched = items.merge(products[['product_id', 'product_category_name']], on='product_id', how='left')

    # 5. Aggregate Items (Order Level)
    order_items_agg = items_enriched.groupby('order_id').agg({
        'price': 'sum',
        'freight_value': 'sum',
        'product_category_name': 'first'  
    }).reset_index()
    
    order_items_agg['total_order_value'] = order_items_agg['price'] + order_items_agg['freight_value']

    # 6. Prepare Reviews
    reviews_agg = reviews.groupby('order_id')['review_score'].mean().reset_index()

    # 7. The Great Merge (Joining 5 Tables)
    print("🔗 Merging datasets...")
    merged_df = orders.merge(order_items_agg, on='order_id', how='left')
    merged_df = merged_df.merge(reviews_agg, on='order_id', how='left')
    final_df = merged_df.merge(customers, on='customer_id', how='inner')

    # 8. Clean Up
    final_df['review_score'] = final_df['review_score'].fillna(0)

    print(f"✅ Final Data Shape: {final_df.shape}")
    print(f"   Unique Customers: {final_df['customer_unique_id'].nunique()}")
    
    return final_df

if __name__ == "__main__":
    df = load_and_clean_data()
    
    # Robust Pathing: Ensure it saves to the correct folder regardless of where the script is run from
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    output_path = os.path.join(project_root, 'data', 'processed', 'clean_data.csv')
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"💾 Saved enriched data to '{output_path}'")
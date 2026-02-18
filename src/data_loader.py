import pandas as pd
import os

def load_and_clean_data(base_path='data/raw/'):
    """
    Loads Olist datasets, cleans dates, filters ghost orders,
    and merges them into a single 'Golden Record' dataframe.
    Includes Reviews (Sentiment) and Products (Strategy).
    """
    print("Starting Data Ingestion...")

    # 1. Load Data
    orders = pd.read_csv(os.path.join(base_path, 'olist_orders_dataset.csv'))
    items = pd.read_csv(os.path.join(base_path, 'olist_order_items_dataset.csv'))
    customers = pd.read_csv(os.path.join(base_path, 'olist_customers_dataset.csv'))
    reviews = pd.read_csv(os.path.join(base_path, 'olist_order_reviews_dataset.csv'))
    products = pd.read_csv(os.path.join(base_path, 'olist_products_dataset.csv'))

    # 2. Fix Date Types
    print("timeline fixing...")
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    
    for col in date_cols:
        orders[col] = pd.to_datetime(orders[col], errors='coerce')

    # 3. Filter "Ghost Orders" (Undelivered)
    initial_count = len(orders)
    orders = orders[orders['order_status'] == 'delivered']
    print(f"Filtered out {initial_count - len(orders)} non-delivered orders.")

    # 4. Enrich Items with Category
    # Join Items + Products to get 'product_category_name'
    items_enriched = items.merge(products[['product_id', 'product_category_name']], on='product_id', how='left')

    # 5. Aggregate Items (Order Level)
    # We sum price, but we also want the "Main Category" of the order.
    # Logic: We take the category of the first item in the order for simplicity.
    order_items_agg = items_enriched.groupby('order_id').agg({
        'price': 'sum',
        'freight_value': 'sum',
        'product_category_name': 'first'  # Takes the first category found in the basket
    }).reset_index()
    
    order_items_agg['total_order_value'] = order_items_agg['price'] + order_items_agg['freight_value']

    # 6. Prepare Reviews
    # Some orders have multiple reviews. We take the average score.
    reviews_agg = reviews.groupby('order_id')['review_score'].mean().reset_index()

    # 7. The Great Merge (Joining 5 Tables)
    # Orders + Items (Money & Category)
    merged_df = orders.merge(order_items_agg, on='order_id', how='left')
    
    # + Reviews (Sentiment)
    merged_df = merged_df.merge(reviews_agg, on='order_id', how='left')
    
    # + Customers (Identity)
    final_df = merged_df.merge(customers, on='customer_id', how='inner')

    # 8. Clean Up
    # Fill missing review scores with the average (approx 4.0) or a specific flag (-1)
    # Here we fill with 0 to indicate "Unknown/No Review"
    final_df['review_score'] = final_df['review_score'].fillna(0)

    print(f"Final Data Shape: {final_df.shape}")
    print(f"   Unique Customers: {final_df['customer_unique_id'].nunique()}")
    
    return final_df

if __name__ == "__main__":
    df = load_and_clean_data()
    df.to_csv('data/processed/clean_data.csv', index=False)
    print("Saved enriched data to 'data/processed/clean_data.csv'")
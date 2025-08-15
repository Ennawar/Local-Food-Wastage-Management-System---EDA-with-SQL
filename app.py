import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

# --- Database Initialization and Data Loading ---
@st.cache_resource # Use st.cache_resource to ensure the database is created only once per app session
def init_db(providers_csv, receivers_csv, food_listings_csv, claims_csv):
    # Set check_same_thread=False for SQLite to work across Streamlit's threading model
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    cursor = conn.cursor()

    # Load CSVs into Pandas DataFrames
    providers_df = pd.read_csv(providers_csv)
    receivers_df = pd.read_csv(receivers_csv)
    food_listings_df = pd.read_csv(food_listings_csv)
    claims_df = pd.read_csv(claims_csv)

    # Convert date columns to datetime objects in Pandas before loading to SQL
    food_listings_df['Expiry_Date'] = pd.to_datetime(food_listings_df['Expiry_Date'])
    claims_df['Timestamp'] = pd.to_datetime(claims_df['Timestamp'])

    # Load Pandas DataFrames into SQLite tables
    providers_df.to_sql('providers', conn, if_exists='replace', index=False)
    receivers_df.to_sql('receivers', conn, if_exists='replace', index=False)
    food_listings_df.to_sql('food_listings', conn, if_exists='replace', index=False)
    claims_df.to_sql('claims', conn, if_exists='replace', index=False)

    return conn

# Initialize the database connection
try:
    conn = init_db('providers_data.csv', 'receivers_data.csv', 'food_listings_data.csv', 'claims_data.csv')
    st.success("Database initialized and data loaded successfully!")
except Exception as e:
    st.error(f"Failed to initialize database: {e}")
    st.info("Please ensure all CSV files are in the same directory as app.py.")
    st.stop()

# --- Helper Functions for SQL Queries ---

def run_query(query, params=()):
    # Pass conn to pd.read_sql_query directly in each function call if not using check_same_thread=False
    # However, with check_same_thread=False, conn can be global as it's handled by SQLite
    return pd.read_sql_query(query, conn, params=params)

def execute_dml(query, params=()):
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()

# --- Functions for specific queries (matching the EDA section) ---

def get_providers_receivers_per_city_sql():
    query = """
    SELECT
        COALESCE(p.City, r.City) AS City,
        COUNT(DISTINCT p.Provider_ID) AS NumProviders,
        COUNT(DISTINCT r.Receiver_ID) AS NumReceivers
    FROM providers p
    LEFT JOIN receivers r ON p.City = r.City
    GROUP BY COALESCE(p.City, r.City)
    ORDER BY NumProviders DESC, NumReceivers DESC;
    """
    return run_query(query)

def get_food_contribution_by_provider_type_sql():
    query = """
    SELECT
        fl.Provider_Type,
        SUM(fl.Quantity) AS TotalQuantity
    FROM food_listings fl
    GROUP BY fl.Provider_Type
    ORDER BY TotalQuantity DESC;
    """
    return run_query(query)

def get_provider_contact_info_sql(city):
    query = f"""
    SELECT
        Name,
        Type,
        Contact
    FROM providers
    WHERE City = '{city}';
    """
    return run_query(query)

def get_top_receivers_by_claimed_food_sql():
    query = """
    SELECT
        r.Name AS Receiver_Name,
        SUM(fl.Quantity) AS TotalClaimedQuantity
    FROM claims c
    JOIN food_listings fl ON c.Food_ID = fl.Food_ID
    JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
    WHERE c.Status = 'Completed'
    GROUP BY r.Name
    ORDER BY TotalClaimedQuantity DESC;
    """
    return run_query(query)

def get_total_food_available_sql():
    query = """
    SELECT SUM(Quantity) AS TotalFoodAvailable FROM food_listings;
    """
    return run_query(query)['TotalFoodAvailable'].iloc[0]

def get_city_with_most_listings_sql():
    query = """
    SELECT
        Location,
        COUNT(Food_ID) AS NumListings
    FROM food_listings
    GROUP BY Location
    ORDER BY NumListings DESC
    LIMIT 1;
    """
    return run_query(query)

def get_most_common_food_types_sql():
    query = """
    SELECT
        Food_Name,
        COUNT(Food_ID) AS NumListings
    FROM food_listings
    GROUP BY Food_Name
    ORDER BY NumListings DESC
    LIMIT 5;
    """
    return run_query(query)

def get_claims_per_food_item_sql():
    query = """
    SELECT
        fl.Food_Name,
        COUNT(c.Claim_ID) AS NumClaims
    FROM claims c
    JOIN food_listings fl ON c.Food_ID = fl.Food_ID
    GROUP BY fl.Food_Name
    ORDER BY NumClaims DESC;
    """
    return run_query(query)

def get_providers_with_successful_claims_sql():
    query = """
    SELECT
        p.Name AS Provider_Name,
        COUNT(c.Claim_ID) AS SuccessfulClaims
    FROM claims c
    JOIN food_listings fl ON c.Food_ID = fl.Food_ID
    JOIN providers p ON fl.Provider_ID = p.Provider_ID
    WHERE c.Status = 'Completed'
    GROUP BY p.Name
    ORDER BY SuccessfulClaims DESC;
    """
    return run_query(query)

def get_claim_status_percentage_sql():
    query = """
    SELECT
        Status,
        CAST(COUNT(Claim_ID) AS REAL) * 100 / (SELECT COUNT(*) FROM claims) AS Percentage
    FROM claims
    GROUP BY Status
    ORDER BY Percentage DESC;
    """
    return run_query(query)

def get_avg_quantity_claimed_per_receiver_sql():
    query = """
    SELECT
        r.Name AS Receiver_Name,
        AVG(fl.Quantity) AS AvgQuantityClaimed
    FROM claims c
    JOIN food_listings fl ON c.Food_ID = fl.Food_ID
JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
    WHERE c.Status = 'Completed'
    GROUP BY r.Name
    ORDER BY AvgQuantityClaimed DESC;
    """
    return run_query(query)

def get_most_claimed_meal_type_sql():
    query = """
    SELECT
        fl.Meal_Type,
        COUNT(c.Claim_ID) AS NumClaims
    FROM claims c
    JOIN food_listings fl ON c.Food_ID = fl.Food_ID
    WHERE c.Status = 'Completed'
    GROUP BY fl.Meal_Type
    ORDER BY NumClaims DESC;
    """
    return run_query(query)

def get_total_food_donated_by_provider_sql():
    query = """
    SELECT
        p.Name AS Provider_Name,
        SUM(fl.Quantity) AS TotalDonatedQuantity
    FROM food_listings fl
    JOIN providers p ON fl.Provider_ID = p.Provider_ID
    GROUP BY p.Name
    ORDER BY TotalDonatedQuantity DESC;
    """
    return run_query(query)

def get_food_nearing_expiry_sql(days=7):
    today_str = datetime.now().strftime('%Y-%m-%d')
    query = f"""
    SELECT
        Food_Name,
        Quantity,
        Expiry_Date,
        Location,
        Provider_ID
    FROM food_listings
    WHERE Expiry_Date > '{today_str}' AND Expiry_Date <= DATE('{today_str}', '+{days} days')
    ORDER BY Expiry_Date ASC;
    """
    return run_query(query)

def get_claims_by_receiver_type_sql():
    query = """
    SELECT
        r.Type AS Receiver_Type,
        COUNT(c.Claim_ID) AS NumClaims
    FROM claims c
    JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
    GROUP BY r.Type
    ORDER BY NumClaims DESC;
    """
    return run_query(query)

# --- CRUD Operations ---

def add_food_listing_sql(food_id, food_name, quantity, expiry_date, provider_id, provider_type, location, food_type, meal_type):
    query = f"""
    INSERT INTO food_listings (Food_ID, Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    execute_dml(query, (food_id, food_name, quantity, expiry_date.strftime('%Y-%m-%d'), provider_id, provider_type, location, food_type, meal_type))

def update_food_listing_sql(food_id, food_name, quantity, expiry_date, food_type, meal_type):
    query = f"""
    UPDATE food_listings
    SET Food_Name = ?, Quantity = ?, Expiry_Date = ?, Food_Type = ?, Meal_Type = ?
    WHERE Food_ID = ?;
    """
    execute_dml(query, (food_name, quantity, expiry_date.strftime('%Y-%m-%d'), food_type, meal_type, food_id))

def delete_food_listing_sql(food_id):
    # Also delete related claims to maintain referential integrity (optional, but good practice)
    execute_dml("DELETE FROM claims WHERE Food_ID = ?;", (food_id,))
    query = "DELETE FROM food_listings WHERE Food_ID = ?;"
    execute_dml(query, (food_id,))

def add_claim_sql(claim_id, food_id, receiver_id, status, timestamp):
    query = f"""
    INSERT INTO claims (Claim_ID, Food_ID, Receiver_ID, Status, Timestamp)
    VALUES (?, ?, ?, ?, ?);
    """
    execute_dml(query, (claim_id, food_id, receiver_id, status, timestamp.strftime('%Y-%m-%d %H:%M:%S')))

# --- Streamlit App Layout ---
st.title("🍽️ Local Food Wastage Management System")
st.markdown("Connecting surplus food to those in need, reducing waste, and combating food insecurity.")

# Sidebar Navigation
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard & Queries", "Food Listings (CRUD)", "Claim Food"])

if page == "Dashboard & Queries":
    st.header("📊 Dashboard & Key Insights")
    st.markdown("Explore key metrics and trends in food donation and claims.")

    # --- Display all 15+ SQL Queries ---
    st.subheader("SQL Query Outputs")

    with st.expander("Q1: Providers and Receivers per City"):
        st.dataframe(get_providers_receivers_per_city_sql())

    with st.expander("Q2: Food Contribution by Provider Type"):
        st.dataframe(get_food_contribution_by_provider_type_sql())

    with st.expander("Q3: Contact Info for Providers in a Specific City"):
        # Get unique cities from the providers table in the database
        cities_in_db = run_query("SELECT DISTINCT City FROM providers ORDER BY City ASC;")['City'].tolist()
        city_filter_q3 = st.selectbox("Select City for Q3", cities_in_db)
        st.dataframe(get_provider_contact_info_sql(city_filter_q3))

    with st.expander("Q4: Top Receivers by Claimed Food Quantity"):
        st.dataframe(get_top_receivers_by_claimed_food_sql())

    with st.expander("Q5: Total Food Available"):
        st.write(f"Total quantity of food available: **{get_total_food_available_sql()} units**")

    with st.expander("Q6: City with Highest Food Listings"):
        st.dataframe(get_city_with_most_listings_sql())

    with st.expander("Q7: Most Commonly Available Food Types"):
        st.dataframe(get_most_common_food_types_sql())

    with st.expander("Q8: Number of Claims per Food Item"):
        st.dataframe(get_claims_per_food_item_sql())

    with st.expander("Q9: Providers with Highest Number of Successful Claims"):
        st.dataframe(get_providers_with_successful_claims_sql())

    with st.expander("Q10: Percentage of Claim Statuses"):
        st.dataframe(get_claim_status_percentage_sql())

    with st.expander("Q11: Average Quantity Claimed per Receiver"):
        st.dataframe(get_avg_quantity_claimed_per_receiver_sql())

    with st.expander("Q12: Most Claimed Meal Type"):
        st.dataframe(get_most_claimed_meal_type_sql())

    with st.expander("Q13: Total Food Donated by Each Provider"):
        st.dataframe(get_total_food_donated_by_provider_sql())

    with st.expander("Q14: Food Items Nearing Expiry (Next 7 Days)"):
        st.dataframe(get_food_nearing_expiry_sql())

    with st.expander("Q15: Number of Claims by Receiver Type"):
        st.dataframe(get_claims_by_receiver_type_sql())

elif page == "Food Listings (CRUD)":
    st.header("📋 Manage Food Listings")
    st.markdown("Add, Update, or Remove food items available for donation.")

    # Fetch current food listings from DB for display and max ID
    current_food_listings_df = run_query("SELECT * FROM food_listings;")
    max_food_id = current_food_listings_df['Food_ID'].max() if not current_food_listings_df.empty else 0

    # --- Add New Listing ---
    st.subheader("➕ Add New Food Listing")
    with st.form("add_listing_form"):
        new_food_id = st.number_input("Food ID", min_value=int(max_food_id + 1), step=1)
        new_food_name = st.text_input("Food Name")
        new_quantity = st.number_input("Quantity", min_value=1)
        new_expiry_date = st.date_input("Expiry Date", min_value=datetime.now().date())
        
        # Select Provider from existing providers in DB
        providers_in_db = run_query("SELECT Provider_ID, Name, Type, City FROM providers;")
        provider_options = providers_in_db.apply(lambda row: f"{row['Name']} ({row['Type']}, {row['City']})", axis=1).tolist()
        selected_provider_full_info = st.selectbox("Select Provider", provider_options)

        # Extract ID, Type, Location from selected provider string
        selected_provider_row = providers_in_db[providers_in_db.apply(lambda row: f"{row['Name']} ({row['Type']}, {row['City']})", axis=1) == selected_provider_full_info].iloc[0]
        new_provider_id = selected_provider_row['Provider_ID']
        new_provider_type = selected_provider_row['Type']
        new_location = selected_provider_row['City']

        new_food_type = st.selectbox("Food Type", ['Vegetarian', 'Non-Vegetarian', 'Vegan'])
        new_meal_type = st.selectbox("Meal Type", ['Breakfast', 'Lunch', 'Dinner', 'Snacks'])

        submitted = st.form_submit_button("Add Listing")
        if submitted:
            if not new_food_name or new_quantity <= 0:
                st.error("Please fill in all required fields (Food Name, Quantity).")
            else:
                add_food_listing_sql(new_food_id, new_food_name, new_quantity, new_expiry_date, new_provider_id, new_provider_type, new_location, new_food_type, new_meal_type)
                st.success(f"Food listing '{new_food_name}' added successfully!")
                st.rerun() # Rerun to update the displayed table

    # --- Update/Delete Listing ---
    st.subheader("✏️ Update / 🗑️ Delete Existing Food Listing")
    
    # Fetch current listings for selection
    listings_for_selection = run_query("SELECT Food_ID, Food_Name FROM food_listings;")
    if not listings_for_selection.empty:
        selected_listing_id = st.selectbox(
            "Select Food ID to Update/Delete",
            listings_for_selection['Food_ID'].tolist(),
            format_func=lambda x: f"{x} - {listings_for_selection[listings_for_selection['Food_ID'] == x]['Food_Name'].iloc[0]}"
        )
    else:
        selected_listing_id = None
        st.info("No listings available to update or delete.")

    if selected_listing_id:
        current_listing_db = run_query(f"SELECT * FROM food_listings WHERE Food_ID = {selected_listing_id};").iloc[0]

        with st.form("update_delete_listing_form"):
            updated_food_name = st.text_input("Food Name", value=current_listing_db['Food_Name'])
            updated_quantity = st.number_input("Quantity", min_value=1, value=int(current_listing_db['Quantity']))
            
            # Convert Expiry_Date string from DB to date object for st.date_input
            # Use pd.to_datetime for robust parsing of date strings from SQLite
            current_expiry_date_obj = pd.to_datetime(current_listing_db['Expiry_Date']).date()
            updated_expiry_date = st.date_input("Expiry Date", value=current_expiry_date_obj)
            
            # Display provider details (not editable here)
            provider_details_db = run_query(f"SELECT Name, Type, City FROM providers WHERE Provider_ID = {current_listing_db['Provider_ID']};").iloc[0]
            st.write(f"Provider: {provider_details_db['Name']} ({provider_details_db['Type']}) in {provider_details_db['City']}")

            updated_food_type = st.selectbox("Food Type", ['Vegetarian', 'Non-Vegetarian', 'Vegan'], index=['Vegetarian', 'Non-Vegetarian', 'Vegan'].index(current_listing_db['Food_Type']))
            updated_meal_type = st.selectbox("Meal Type", ['Breakfast', 'Lunch', 'Dinner', 'Snacks'], index=['Breakfast', 'Lunch', 'Dinner', 'Snacks'].index(current_listing_db['Meal_Type']))

            col1, col2 = st.columns(2)
            update_button = col1.form_submit_button("Update Listing")
            delete_button = col2.form_submit_button("Delete Listing")

            if update_button:
                update_food_listing_sql(selected_listing_id, updated_food_name, updated_quantity, updated_expiry_date, updated_food_type, updated_meal_type)
                st.success(f"Food listing '{updated_food_name}' (ID: {selected_listing_id}) updated successfully!")
                st.rerun()

            if delete_button:
                delete_food_listing_sql(selected_listing_id)
                st.warning(f"Food listing (ID: {selected_listing_id}) deleted successfully!")
                st.rerun()
    
    st.subheader("Current Food Listings")
    st.dataframe(run_query("SELECT * FROM food_listings;"))


elif page == "Claim Food":
    st.header("Claim Available Food")
    st.markdown("Browse available food listings and claim items you need.")

    # --- Filtering Options ---
    st.subheader("🔍 Filter Food Listings")
    col1, col2, col3, col4 = st.columns(4)
    
    # Get unique filter options directly from the database
    all_locations = run_query("SELECT DISTINCT Location FROM food_listings ORDER BY Location ASC;")['Location'].tolist()
    all_provider_types = run_query("SELECT DISTINCT Provider_Type FROM food_listings ORDER BY Provider_Type ASC;")['Provider_Type'].tolist()
    all_food_types = run_query("SELECT DISTINCT Food_Type FROM food_listings ORDER BY Food_Type ASC;")['Food_Type'].tolist()
    all_meal_types = run_query("SELECT DISTINCT Meal_Type FROM food_listings ORDER BY Meal_Type ASC;")['Meal_Type'].tolist()


    with col1:
        selected_city = st.selectbox("Filter by City", ['All'] + all_locations)
    with col2:
        selected_provider_type = st.selectbox("Filter by Provider Type", ['All'] + all_provider_types)
    with col3:
        selected_food_type = st.selectbox("Filter by Food Type", ['All'] + all_food_types)
    with col4:
        selected_meal_type = st.selectbox("Filter by Meal Type", ['All'] + all_meal_types)

    # Build SQL query for filtered listings
    filter_query = """
    SELECT
        fl.Food_ID,
        fl.Food_Name,
        fl.Quantity,
        fl.Expiry_Date,
        fl.Provider_Type,
        fl.Location,
        fl.Food_Type,
        fl.Meal_Type,
        p.Name AS Provider_Name,
        p.Contact AS Provider_Contact
    FROM food_listings fl
    JOIN providers p ON fl.Provider_ID = p.Provider_ID
    WHERE 1=1
    """
    params = []

    if selected_city != 'All':
        filter_query += " AND fl.Location = ?"
        params.append(selected_city)
    if selected_provider_type != 'All':
        filter_query += " AND fl.Provider_Type = ?"
        params.append(selected_provider_type)
    if selected_food_type != 'All':
        filter_query += " AND fl.Food_Type = ?"
        params.append(selected_food_type)
    if selected_meal_type != 'All':
        filter_query += " AND fl.Meal_Type = ?"
        params.append(selected_meal_type)
    
    filter_query += " ORDER BY fl.Expiry_Date ASC, fl.Food_Name ASC;"

    filtered_listings_df = run_query(filter_query, params)

    st.subheader("Available Food Items")
    if not filtered_listings_df.empty:
        st.dataframe(filtered_listings_df)
    else:
        st.info("No food listings match your criteria.")

    # --- Claim Food ---
    st.subheader("Claim a Food Item")
    
    # Get max claim ID for new claim
    max_claim_id = run_query("SELECT MAX(Claim_ID) FROM claims;").iloc[0,0]
    new_claim_id = int(max_claim_id + 1) if pd.notna(max_claim_id) else 1

    with st.form("claim_form"):
        claim_food_id = st.number_input("Enter Food ID to Claim", min_value=1, step=1)
        
        # Select Receiver from existing receivers in DB
        receivers_in_db = run_query("SELECT Receiver_ID, Name, Type FROM receivers;")
        receiver_options = receivers_in_db.apply(lambda row: f"{row['Name']} ({row['Type']})", axis=1).tolist()
        selected_receiver_full_info = st.selectbox("Select Your Name (Receiver)", receiver_options)
        
        # Extract ID from selected receiver string
        selected_receiver_row = receivers_in_db[receivers_in_db.apply(lambda row: f"{row['Name']} ({row['Type']})", axis=1) == selected_receiver_full_info].iloc[0]
        claim_receiver_id = selected_receiver_row['Receiver_ID']

        claim_status = st.selectbox("Claim Status", ['Pending', 'Completed', 'Cancelled'])
        
        claim_button = st.form_submit_button("Submit Claim")

        if claim_button:
            # Check if Food ID exists
            food_exists_query = f"SELECT COUNT(*) FROM food_listings WHERE Food_ID = {claim_food_id};"
            food_exists = run_query(food_exists_query).iloc[0,0]
            
            if food_exists == 0:
                st.error("Invalid Food ID. Please enter an existing Food ID.")
            else:
                add_claim_sql(new_claim_id, claim_food_id, claim_receiver_id, claim_status, datetime.now())
                st.success(f"Claim for Food ID {claim_food_id} submitted as '{claim_status}'!")
                st.rerun() # Rerun to update the displayed table
    
    st.subheader("Your Claims History")
    # Fetch claims data from DB for display
    claims_display_query = """
    SELECT
        c.Claim_ID,
        fl.Food_Name,
        r.Name AS Receiver_Name,
        c.Status,
        c.Timestamp
    FROM claims c
    JOIN food_listings fl ON c.Food_ID = fl.Food_ID
    JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
    ORDER BY c.Timestamp DESC;
    """
    st.dataframe(run_query(claims_display_query))

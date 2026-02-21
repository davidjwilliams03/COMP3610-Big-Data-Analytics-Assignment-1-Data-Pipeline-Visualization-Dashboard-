import requests
import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
from pathlib import Path

# -----------------------------------------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------------------------------------

st.set_page_config(
    page_title="New York City Yellow Taxi Analytics Dashboard",
    layout="wide"
)

st.title("New York City Yellow Taxi Data Dashboard: January 2024")
st.markdown("""
An analytical overview of NYC Yellow Taxi trip data from January 2024.
Trip demand patterns, fare behaviour, payment methods, and temporal travel trends are explored.
These results are based on cleaned and validated trip records.
""")

# -----------------------------------------------------------------------------------------------------------
# Data Loading with Caching (Optimized for Streamlit)
# -----------------------------------------------------------------------------------------------------------
raw_data_path = Path("data/raw")
raw_data_path.mkdir(parents=True, exist_ok=True)

# URLs
trip_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

trip_file = raw_data_path / "yellow_tripdata_2024_01.parquet"
zone_file = raw_data_path / "taxi_zone_lookup.csv"

def download_file(url, destination):
    if not destination.exists():
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise Exception(f"Failed to download {url}")
        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {destination}")
    else:
        print(f"File already exists: {destination}")


@st.cache_data
def load_data():
    """
    Loads and cleans the taxi dataset.
    Uses Polars lazy mode so we don’t load millions of rows into memory at once.
    The function is cached so Streamlit does not reload the dataset on every interaction.
    """
     # Download data if missing
    download_file(trip_url, trip_file)
    download_file(zone_url, zone_file)

    data_path = trip_file
    zone_path = zone_file
    #data_path = "data/raw/yellow_tripdata_2024_01.parquet"
    #zone_path = "data/raw/taxi_zone_lookup.csv"

    # I Used scan_parquet (Lazy Mode) to prevent memory crashes that occurred 
    # This keeps the millions of rows on disk until we filter them
    lazy_df = pl.scan_parquet(data_path)

    # Data Cleaning to remove rows with missing columns
    lazy_df = lazy_df.drop_nulls([
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "trip_distance",
        "fare_amount"
    ])

    lazy_df = lazy_df.filter( # to filter unrealistic and invalid trips. (error entries as well) 
        (pl.col("trip_distance") > 0) &
        (pl.col("trip_distance") <= 100) &
        (pl.col("fare_amount") > 0) &
        (pl.col("fare_amount") <= 500) &
        (pl.col("tpep_dropoff_datetime") >= pl.col("tpep_pickup_datetime"))
    )

    # Feature Engineering 
    lazy_df = lazy_df.with_columns([
        
        # Trip duration in minutes
        ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
         .dt.total_minutes()).alias("trip_duration_minutes"),

        # Trip speed in miles per hour
        (pl.col("trip_distance") /
         ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
          .dt.total_minutes() / 60)).alias("trip_speed_mph"),

        # Extract hour of pickup
        pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),

        # Extract weekday name
        pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("pickup_day_of_week")
        
    ])

    # Selecting only the columns that are needed in the dashboard to keep memory low
    cols = ["tpep_pickup_datetime", 
            "fare_amount", 
            "total_amount", 
            "PULocationID", 
            "trip_distance", 
            "trip_duration_minutes", 
            "pickup_hour", 
            "pickup_day_of_week", 
            "payment_type"
           ]
    
    # Collect the processed data into memory after the filtering and selecting is done
    df = lazy_df.select(cols).collect()
    zones = pl.read_csv(zone_path) # zone lookup table is loaded

    return df, zones

# Initialize data as Polars objects
df, zones = load_data()

# -----------------------------------------------------------------------------------------------------------
# Sidebar Filters
# -----------------------------------------------------------------------------------------------------------

import datetime
st.sidebar.header("Filter by")

# Get min/max from Polars efficiently
jan_start = datetime.date(2024, 1, 1) # this was done because somehow it was pulling data from 2002
jan_end = datetime.date(2024, 1, 31) # this necessitated me hardcoding this part

#min_date = df["tpep_pickup_datetime"].min().date()
#max_date = df["tpep_pickup_datetime"].max().date()

date_range = st.sidebar.date_input(
    "Select Date Range",
    [jan_start, jan_end],  # Default to just the month of Jan
    min_value=jan_start,   # Prevent selecting 2002
    max_value=jan_end
)

hour_range = st.sidebar.slider(
    "Select Hour Range",
    min_value=0,
    max_value=23,
    value=(0, 23)
)

payment_map = { # mapping the vague payment codes to readable labels
    1: 'Credit Card', 
    2: 'Cash', 
    0: 'Void/Unknown', 
    4: 'Dispute', 
    3: 'No Charge'
}

payment_ids = df["payment_type"].unique().to_list()

payment_options = sorted(
    [payment_map.get(i, f"ID {i}") for i in payment_ids]
)

selected_payments = st.sidebar.multiselect(
    "Select Payment Type",
    payment_options,
    default=payment_options
)

selected_ids = [
    k for k, v in payment_map.items() 
    if v in selected_payments
]

# -----------------------------------------------------------------------------------------------------------
# Apply Filters (The Memory Secret)
# -----------------------------------------------------------------------------------------------------------

# 1. Filter in Polars first (takes 0.1 seconds, so it's way faster)
filtered_polars = df.filter(
    (pl.col("tpep_pickup_datetime").dt.date() >= date_range[0]) &
    (pl.col("tpep_pickup_datetime").dt.date() <= date_range[1]) &
    (pl.col("pickup_hour") >= hour_range[0]) &
    (pl.col("pickup_hour") <= hour_range[1]) &
    (pl.col("payment_type").is_in(selected_ids)) &
    (pl.col("trip_distance") > 0) & (pl.col("trip_distance") <= 100)    
)

# 2. Converting only the smaller filtered data to Pandas for plotting
filtered_df = filtered_polars.to_pandas()

# -----------------------------------------------------------------------------------------------------------
# Key Metrics
# -----------------------------------------------------------------------------------------------------------

if not filtered_df.empty:
    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Total Trips", f"{len(filtered_df):,}")
    col2.metric("Average Fare ($)", f"{filtered_df['fare_amount'].mean():.2f}")
    col3.metric("Total Revenue ($)", f"{filtered_df['total_amount'].sum():,.0f}")
    col4.metric("Avg Distance (mi)", f"{filtered_df['trip_distance'].mean():.2f}")
    col5.metric("Avg Duration (min)", f"{filtered_df['trip_duration_minutes'].mean():.2f}")

    st.markdown("---")

# -----------------------------------------------------------------------------------------------------------
# Tabs Layout
# -----------------------------------------------------------------------------------------------------------
st.markdown("""
    <style>
    /* 1. Center the entire tab list and make it equidistant */
    .stTabs [data-baseweb="tab-list"] {
        display: flex;
        justify-content: center; /* Centers the group of tabs */
        gap: 2vw;               /* Creates equal, responsive distance between tabs */
        width: 100%;
    }

    /* 2. Style the individual tab buttons */
    .stTabs [data-baseweb="tab"] {
        flex-grow: 1;           /* Allows tabs to expand equally */
        text-align: center;
        max-width: 300px;       /* Prevents tabs from becoming too wide on huge screens */
    }

    /* 3. Responsive font sizing */
    .stTabs [data-baseweb="tab"] p {
        font-size: clamp(16px, 1.5vw, 24px); /* Scales between 16px and 24px based on screen width */
        font-weight: bold;
        white-space: nowrap;    /* Keeps text on one line */
    }
    </style>
    """, 
    unsafe_allow_html=True
)

tab1, tab2, tab3 = st.tabs(["Demand Patterns", "Fare & Payments", "Temporal Trends"])

# -----------------------------------------------------------------------------------------------------------
# TAB 1 — Demand Patterns
# -----------------------------------------------------------------------------------------------------------

with tab1:
    top_zones = (
        filtered_df
        .merge(zones.to_pandas(), left_on="PULocationID", right_on="LocationID")
        .groupby("Zone")
        .size()
        .reset_index(name="trip_count")
        .sort_values("trip_count", ascending=False)
        .head(10)
    )

    fig1 = px.bar(
       top_zones,
        x="Zone",
        y="trip_count",
        title="Top 10 Pickup Zones",
        template="plotly_white"
    )
    st.plotly_chart(fig1, use_container_width=True)

    st.markdown("""
    
### The "Big Three" Hubs  

There is a clear tier of top-performing zones. Midtown Centre, Upper East Side South, and JFK Airport are the dominant leaders, each surpassing 130k trips. These areas represent the primary engines of NYC transit: the central business district (Midtown), high-density residential wealth (UES), and international travel (JFK).
  
  
  
### The Airport Contrast  

Interestingly, there is a significant gap between the two major airports shown. JFK Airport is the 3rd most popular pickup zone overall. LaGuardia Airport ranks 9th, with significantly fewer trips (below the 100k mark). This suggests travellers at JFK are more reliant on these specific services, or simply reflect JFK’s higher passenger volume compared to LaGuardia.



### Concentration in Upper Manhattan & Midtown  

The list is heavily skewed toward Manhattan’s "Core." Zones like Midtown East, Times Square, and Penn Station all hover right around the 100k trip line. The Upper East Side appears twice (North and South), suggesting that if combined, the UES would likely be the single most active geographic area for pickups in the city.


### Transit Intersections  

The presence of Penn Station/Madison Square West and Times Square/Theatre District in the top 10 highlights "last-mile" behaviour. Many trips likely originate from commuters arriving via regional rail (LIRR/NJ Transit) or subway hubs who then take a vehicle to their final destination. Taxi demand is highly concentrated in specific zones, likely representing commercial hubs, transportation centres, and high-density residential areas. The dominance of top zones indicates strong geographic clustering of taxi activity.
    """)

# -----------------------------------------------------------------------------------------------------------
# TAB 2 — Fare & Payments
# -----------------------------------------------------------------------------------------------------------

with tab2:
    avg_fare_hour = (
        filtered_df
        .groupby("pickup_hour")["fare_amount"]
        .mean()
        .reset_index()
    )

    fig2 = px.line(
        avg_fare_hour,
        x="pickup_hour",
        y="fare_amount",
        title="Average Fare by Hour",
        template="plotly_white"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.markdown("""
### The 5:00 AM "Super Peak"  

There is a dramatic spike in average fares starting around 3:00 AM and peaking sharply at 5:00 AM, reaching nearly $28. This is the highest average fare of the entire day. It likely reflects long-distance trips to airports (JFK/LaGuardia) for early-morning flights or "end-of-night" trips where supply is low, and distances might be longer than mid-day city hops.  


    
### The Mid-Day Plateau  

Following the morning spike, fares drop rapidly and stabilise between 8:00 AM and 1:00 PM, hovering in the $18 range. Despite high traffic volume during these hours, the average fare is lower. This suggests a high density of short-distance "commuter" trips within Manhattan, which keeps the average price down compared to the early morning outliers.  


    
### The Afternoon "Mini-Peak" 

There is a slight increase in fares between 2:00 PM (14:00) and 4:00 PM (16:00), reaching roughly $19.50. This likely correlates with the shift change for many taxi drivers or the beginning of the school/work wind-down, where increased demand or traffic congestion begins to push fares up slightly.  

  

### 4. The 6:00 PM Dip 

Interestingly, there is a noticeable dip around 6:00 PM, where the fare drops to its second-lowest point of the day at approximately $17. This is counterintuitive for "rush hour." It may indicate a shift toward very short, local trips as people head to dinner or nearby social events after work, or a high concentration of traffic that limits the distance (and thus the fare) of each trip.
    """)


    payment_breakdown = (
        filtered_df["payment_type"]
        .value_counts(normalize=True)
        .reset_index()
    )

    payment_breakdown.columns = ['payment_type', 'proportion']
        
    payment_map = {1: 'Credit Card', 2: 'Cash', 0: 'Void/Unknown', 4: 'Dispute', 3: 'No Charge'}
    payment_breakdown['payment_type_labelled'] = payment_breakdown['payment_type'].map(payment_map)
        
    fig3 = px.pie(
        payment_breakdown,
        names="payment_type_labelled",
        values="proportion",
        title="Payment Type Distribution",
        template="plotly_white"
    )
    fig3.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown("""
### Digital Dominance  

Over 80% of passengers prefer credit cards. This aligns with modern urban transit trends where digital payments provide convenience and automated receipt tracking for business travellers.  

  

### Low Cash Usage 

At roughly 15%, cash is no longer the primary way New Yorkers pay for cabs. This may be due to the widespread adoption of in-cab payment screens.  

  

### Minimal Disputes  

The extremely low rate of disputes (0.79%) indicates a high level of fare transparency or general passenger satisfaction with the billing process.  

  

### Data Accuracy Note  

Tips are automatically recorded for credit card payments but are generally not included in the data for cash payments, which might slightly skew total revenue comparisons between the two.
    """)


# -----------------------------------------------------------------------------------------------------------
# TAB 3 — Temporal Trends
# -----------------------------------------------------------------------------------------------------------

with tab3:
    # Define day order to prevent alphabetical sorting (Friday, Monday...)
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        
    heatmap_data = (
        filtered_df
        .groupby(["pickup_day_of_week", "pickup_hour"])
        .size()
        .reset_index(name="trip_count")
        .pivot(index="pickup_day_of_week",
             columns="pickup_hour",
             values="trip_count")
        .reindex(day_order) # This ensures Monday is at the top, Sunday at the bottom
        .fillna(0)
    )

    fig4 = px.imshow(
        heatmap_data,
        aspect="auto",
        title="Trips by Day and Hour",
        template="plotly_white"
    )
    st.plotly_chart(fig4, use_container_width=True)

    st.markdown("""
### The Tuesday-Wednesday Peak  

The brightest "yellow" spots, indicating the highest trip volume (over 30k trips), occur on Tuesday and Wednesday evenings, specifically between 6:00 PM (18:00) and 8:00 PM (20:00). Mid-week is actually busier for rides than Mondays or Fridays. This likely reflects peak "after-work" activity, including business dinners and mid-week social events.  

  

### The Weekend "Late Night" Shift  

Look at the bottom-left and top-left corners (Hours 0–4). On Saturday and Sunday mornings, there is significantly higher activity between Midnight and 4:00 AM compared to any weekday. While weekdays are "dead" in the early morning (the deep blue areas), the weekend demand stays elevated much longer into the night, reflecting the city’s nightlife and the transition from Saturday night into Sunday morning.  

  

### The Friday "Early Exit"  

On Friday, demand starts to ramp up earlier in the afternoon (around 2:00 PM / 14:00) compared to Monday or Tuesday. This suggests a "Friday effect" where people leave work early or start their weekend travel sooner than they do on other workdays.  

  

### Sunday’s Unique Pattern  

Sunday has a very different "signature" than the rest of the week. It has the lowest peak volume overall (fewer bright orange/yellow areas). Activity is more evenly spread throughout the afternoon rather than having a sharp evening spike. Sunday demand is likely driven by leisure and personal errands rather than the rigid schedules of the 9-to-5 work week.
    """)

    fig5 = px.histogram(
        filtered_df,
        x="trip_distance",
        nbins=50,
        title="Trip Distance Distribution",
        template="plotly_white"
    )
    st.plotly_chart(fig5, use_container_width=True)
    st.markdown("""
### The "Under 5-Mile" Dominance  

The most prominent peak in the data occurs between 1 and 2 miles, with a slight tapering off by 3 miles. This confirms that the primary use case for these vehicles is local transit within a single borough (likely Manhattan) or "last-mile" travel from transit hubs like Penn Station to an office. Where walking is too far, but the distance doesn't warrant a subway ride.  

  

### The "Long Tail" of the City  

There is a sharp "drop-off" in trip counts as the distance increases beyond 5 miles. However, the data continues all the way out to 50+ miles. These are rare but high-value trips. Journeys in the 15–20 mile range likely represent trips to the outer reaches of the boroughs or the major airports (JFK is roughly 15-20 miles from Midtown). The median trip distance in this type of NYC dataset is typically around 2.0 miles, confirming that most rides are quite short. Because the data was capped at 100 miles, extreme outliers were removed (like accidental GPS pings) while retaining legitimate "out-of-city" hauls to the suburbs or nearby regions.  

  

### Impact on Revenue vs. Volume  

This chart perfectly explains why your Average Fare chart showed a spike at 5:00 AM. Even though there are very few "long" trips (the blue area is almost flat after 20 miles), those trips carry much higher fares. These trips typically represent airport runs. While these trips are far less frequent than the 2-mile rides, they are significantly more profitable due to the higher base fare and flat-rate pricing for certain zones. If you are a driver, one trip in the 20-mile bin is worth significantly more than five trips in the 2-mile bin, but the 2-mile trips are much easier to find and stack.  

  

### Data Distribution Note  

This data follows a log-normal distribution. Because trip distances cannot be negative and we have filtered out the 0.0-mile "non-trips," the data "piles up" immediately after zero and tapers off as distance increases. This is a characteristic signature of urban mobility data.
    """)

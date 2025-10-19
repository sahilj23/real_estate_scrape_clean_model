import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import json
import time
import random
import os
from tqdm import tqdm
import numpy as np

# ----------------- CONFIGURATION -----------------
INPUT_CSV = "cleaned_noida_listings.csv"
OUTPUT_CSV = "scraped_data.csv"
CACHE_FILE = "geocode_cache.json"

GEOLOCATOR_USER_AGENT = "sj_sahil23_real_estate_project"
geolocator = Nominatim(user_agent=GEOLOCATOR_USER_AGENT, timeout=5)# -------------------------------------------------

# ----------------- NOIDA METRO STATIONS (Reference Data) -----------------
NOIDA_METRO_STATIONS = {
    "Noida Sector 15": (28.5833, 77.3117),
    "Noida Sector 16": (28.5786, 77.3168),
    "Noida Sector 18": (28.5714, 77.3259),
    "Botanical Garden": (28.5642, 77.3323),
    "Golf Course": (28.5582, 77.3456),
    "Noida City Centre": (28.5577, 77.3551),
    "Noida Sector 34": (28.5606, 77.3639),
    "Noida Sector 52": (28.5731, 77.3664),
    "Noida Sector 59": (28.5919, 77.3695),
    "Noida Sector 61": (28.5997, 77.3685),
    "Noida Sector 62": (28.6146, 77.3666),
    "Noida Electronic City": (28.6272, 77.3689),
    "Noida Sector 51": (28.5746, 77.3653),
    "Noida Sector 50": (28.5663, 77.3667),
    "Noida Sector 76": (28.5552, 77.3698),
    "Noida Sector 101": (28.5447, 77.3732),
    "Noida Sector 81": (28.5348, 77.3732),
    "NSEZ": (28.5244, 77.3712),
    "Noida Sector 83": (28.5147, 77.3714),
    "Noida Sector 137": (28.5035, 77.3820),
    "Noida Sector 142": (28.4977, 77.3916),
    "Noida Sector 143": (28.4912, 77.4018),
    "Noida Sector 144": (28.4842, 77.4124),
    "Noida Sector 145": (28.4776, 77.4223),
    "Noida Sector 146": (28.4716, 77.4320),
    "Noida Sector 147": (28.4651, 77.4422),
    "Noida Sector 148": (28.4589, 77.4526),
    "Knowledge Park II": (28.4619, 77.4764),
    "Pari Chowk": (28.4650, 77.4883),
    "Alpha 1": (28.4674, 77.4996),
    "Delta 1": (28.4687, 77.5109),
    "GNIDA Office": (28.4679, 77.5218),
    "Depot": (28.4608, 77.5252),
    "Kalindi Kunj": (28.5484, 77.3155),
    "Okhla Bird Sanctuary": (28.5583, 77.3230)
}
# -------------------------------------------------------------------------


# Loading the geocoding cache for persistent API saving
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, 'r') as f:
            geocode_cache = json.load(f)
        print(f"[CACHE] Loaded {len(geocode_cache)} entries from cache.")
    except json.JSONDecodeError:
        print("[CACHE] Error reading cache. Starting with empty cache.")
        geocode_cache = {}
else:
    geocode_cache = {}
    print("[CACHE] Starting with empty cache.")


def get_coordinates(locality_name, city="Noida, India"):
    """
    Looks up coordinates in the cache first. If not found, calls the Nominatim API (Forward Geocoding).
    """
    search_query = f"{locality_name}, {city}"

    if search_query in geocode_cache:
        return geocode_cache[search_query]

    # Implementing strict rate limiting for Nominatim (1 request/sec max)
    time.sleep(random.uniform(1.5, 2.0))

    try:
        # Geocode the locality
        location = geolocator.geocode(search_query)

        if location:
            output = {'lat': location.latitude, 'lng': location.longitude}
        else:
            output = {'lat': np.nan, 'lng': np.nan}

        # Updating cache and saveing immediately to file
        geocode_cache[search_query] = output
        with open(CACHE_FILE, 'a') as f:
            json.dump({search_query: output}, f, indent=4)

        return output

    except Exception as e:
        print(f"\n[ERROR] Nominatim failed for '{search_query}'. Error: {e}")
        return {'lat': np.nan, 'lng': np.nan}


# ***************************************************************
# REVERSE GEOCODING FOR PINCODE
# ***************************************************************
# Reverse cache will store results from (lat, lng) lookups to save time
REVERSE_CACHE_FILE = "reverse_geocode_cache.json"
if os.path.exists(REVERSE_CACHE_FILE):
    with open(REVERSE_CACHE_FILE, 'r') as f:
        reverse_geocode_cache = json.load(f)
    print(f"[REVERSE CACHE] Loaded {len(reverse_geocode_cache)} entries.")
else:
    reverse_geocode_cache = {}


def get_pincode(lat, lng):
    """
    Reverse geocodes coordinates to get the official Pincode (postcode).
    """
    if pd.isna(lat) or pd.isna(lng):
        return np.nan

    query = f"{lat},{lng}"
    if query in reverse_geocode_cache:
        return reverse_geocode_cache[query]

    # Implement strict rate limiting for Nominatim
    time.sleep(random.uniform(1.5, 2.0))

    try:
        location = geolocator.reverse((lat, lng))

        # Check if address components are available and extract postcode
        if location and location.raw and 'address' in location.raw:
            pincode = location.raw['address'].get('postcode', np.nan)

            # Store result in cache
            reverse_geocode_cache[query] = pincode
            with open(REVERSE_CACHE_FILE, 'w') as f:
                json.dump(reverse_geocode_cache, f, indent=4)

            return pincode

        reverse_geocode_cache[query] = np.nan
        return np.nan

    except Exception as e:
        # Catch timeout, block, or connection errors
        print(f"\n[ERROR] Reverse geocoding failed for {query}. Error: {e}")
        return np.nan


# ***************************************************************


def calculate_nearest_metro_distance(lat, lng):
    """
    Calculates the distance in kilometers from the property coordinates to the nearest metro station.
    """
    if pd.isna(lat) or pd.isna(lng):
        return np.nan

    property_coords = (lat, lng)
    min_distance = float('inf')

    for station_name, station_coords in NOIDA_METRO_STATIONS.items():
        # Calculate the distance using the geodesic (great-circle) method for accuracy
        distance = geodesic(property_coords, station_coords).km

        if distance < min_distance:
            min_distance = distance

    return min_distance if min_distance != float('inf') else np.nan


if __name__ == "__main__":
    # Loading imputed data
    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        print(f"Error: {INPUT_CSV} not found. Ensure your clean file is in the current directory.")
        exit()

    # --- 1. FORWARD GEOCODING (lat/lng) ---
    print(f"Starting forward geocoding for {len(df)} listings...")
    unique_localities = df['locality'].dropna().unique()

    for locality in tqdm(unique_localities, desc="Forward Geocoding"):
        get_coordinates(locality)

    print("Applying coordinates to DataFrame...")
    df['search_query'] = df['locality'].apply(lambda x: f"{x}, Noida, India" if pd.notna(x) else None)
    df['latitude'] = df['search_query'].apply(lambda x: geocode_cache.get(x, {}).get('lat'))
    df['longitude'] = df['search_query'].apply(lambda x: geocode_cache.get(x, {}).get('lng'))
    df = df.drop(columns=['search_query'])

    # --- 2. REVERSE GEOCODING (official_pincode) ---
    print("\nStarting reverse geocoding for validation (Pincode)...")
    tqdm.pandas(desc="Reverse Geocoding")
    # Only reverse geocode non-missing lat/lng pairs
    df['official_pincode'] = df.progress_apply(
        lambda row: get_pincode(row['latitude'], row['longitude'])
        if pd.notna(row['latitude']) else np.nan, axis=1
    )

    # --- 3. PROXIMITY CALCULATION (dist_to_nearest_metro_km) ---
    print("\nCalculating nearest metro distance...")
    tqdm.pandas(desc="Metro Distance Calculation")
    df['dist_to_nearest_metro_km'] = df.progress_apply(
        lambda row: calculate_nearest_metro_distance(row['latitude'], row['longitude']), axis=1
    )

    # --- 4. FINAL CLEANUP & IMPUTATION ---
    print("\nFinal cleanup and imputation of new features...")
    for col in ['latitude', 'longitude', 'dist_to_nearest_metro_km', 'official_pincode']:
        if col in df.columns:
            # Pincode should be treated as a string/category
            if col == 'official_pincode':
                df[col] = df[col].astype(str).replace('nan', df[col].mode()[0] if not df[col].mode().empty else 'N/A')
            else:
                # Impute remaining numeric NaNs with the median
                try:
                    df[col].fillna(df[col].median(), inplace=True)
                except:
                    df[col].fillna(0, inplace=True)  # Fallback to 0 if median fails (e.g., all NaN)

    # 5. FINAL SAVE
    print(f"\n[SUCCESS] Geocoding and Feature Engineering complete. Data saved to {OUTPUT_CSV}")
    print(f"New columns added: latitude, longitude, dist_to_nearest_metro_km, official_pincode.")
    df.to_csv(OUTPUT_CSV, index=False)
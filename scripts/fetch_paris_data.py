#!/usr/bin/env python3
"""
Data fetching script for Paris garbage flow visualization.
Focuses on 14th arrondissement (Rive Gauche) waste management data.
"""

import requests
import pandas as pd
import geopandas as gpd
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

class ParisDataFetcher:
    """Fetches and processes Paris open data related to waste management."""
    
    BASE_DATA_DIR = Path("data")
    RAW_DATA_DIR = BASE_DATA_DIR / "raw"
    PROCESSED_DATA_DIR = BASE_DATA_DIR / "processed"
    
    # Paris Open Data API endpoints
    OPENDATA_PARIS_BASE = "https://opendata.paris.fr/api/records/1.0"
    
    # Key datasets for waste management (expanded from Annex 1)
    DATASETS = {
        # Generation & tonnages
        'waste_per_capita': 'quantite-de-dechets-produits-et-tries-par-habitant-et-par-an',
        
        # Drop-off infrastructure
        'glass_igloos': 'dechets-menagers-points-dapport-volontaire-colonnes-a-verre',
        'trilib_stations': 'dechets-menagers-points-dapport-volontaire-stations-trilib',
        'public_composters': 'dechets-menagers-points-dapport-volontaire-composteurs',
        'textile_containers': 'dechets-menagers-points-dapport-volontaire-conteneur-textile',
        'recycling_centers': 'dechets-menagers-points-dapport-volontaire-recycleries-et-ressourceries',
        
        # Public litter bins
        'street_bins': 'plan-de-voirie-mobiliers-urbains-jardinieres-bancs-corbeilles-de-rue',
        
        # Citizen feedback
        'citizen_reports': 'dans-ma-rue',
        
        # Context & routing
        'arrondissement_boundaries': 'arrondissements',
        'neighborhoods': 'quartier_paris',
        'road_network': 'voie',
        
        # Environmental context
        'ghg_emissions': 'inventaire-des-emissions-de-gaz-a-effet-de-serre-du-territoire',
        
        # Legacy datasets (keeping for compatibility)
        'waste_collection_points': 'points-dapport-volontaire',
        'waste_treatment_facilities': 'equipements-de-traitement-des-dechets',
        'waste_statistics': 'tonnages-des-dechets-collectes'
    }
    
    def __init__(self):
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary data directories."""
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        
    def fetch_dataset(self, dataset_key: str, filters: Optional[Dict] = None) -> Optional[pd.DataFrame]:
        """
        Fetch a dataset from Paris Open Data API with improved error handling.
        
        Args:
            dataset_key: Key from DATASETS dict
            filters: Optional filters to apply to the API query
            
        Returns:
            DataFrame with the fetched data
        """
        if dataset_key not in self.DATASETS:
            print(f"Warning: Unknown dataset key '{dataset_key}'. Available keys: {list(self.DATASETS.keys())}")
            return None
            
        dataset_id = self.DATASETS[dataset_key]
        url = f"{self.OPENDATA_PARIS_BASE}/search/"
        
        params = {
            'dataset': dataset_id,
            'rows': 10000,  # Maximum rows
            'format': 'json'
        }
        
        if filters:
            # Add geographic filter for arrondissement (flexible field names)
            if 'arrondissement' in filters:
                arr_value = filters['arrondissement']
                # Try different field names for arrondissement filtering
                possible_fields = ['c_ar', 'arrondissement', 'code_postal']
                for field in possible_fields:
                    params[f'refine.{field}'] = arr_value
                    
        try:
            print(f"Fetching {dataset_key} from {dataset_id}...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            records = data.get('records', [])
            
            if not records:
                print(f"No records found for {dataset_key}")
                # Try without filters if no results
                if filters:
                    print(f"Retrying {dataset_key} without filters...")
                    return self.fetch_dataset(dataset_key, filters=None)
                return None
                
            # Extract fields from records with validation
            processed_records = []
            for record in records:
                fields = record.get('fields', {})
                geometry = record.get('geometry')
                
                # Add geometry if available
                if geometry:
                    fields['geometry'] = geometry
                    
                # Add record metadata
                fields['_record_id'] = record.get('recordid')
                fields['_dataset'] = dataset_key
                
                processed_records.append(fields)
                
            df = pd.DataFrame(processed_records)
            
            # Data validation
            if len(df.columns) == 0:
                print(f"Warning: {dataset_key} has no data columns")
                return None
                
            # Save raw data
            raw_file = self.RAW_DATA_DIR / f"{dataset_key}.json"
            with open(raw_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            print(f"✓ Fetched {len(df)} records for {dataset_key} ({len(df.columns)} columns)")
            return df
            
        except requests.Timeout:
            print(f"✗ Timeout fetching {dataset_key}")
            return None
        except requests.RequestException as e:
            print(f"✗ Error fetching {dataset_key}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"✗ JSON decode error for {dataset_key}: {e}")
            return None
        except Exception as e:
            print(f"✗ Unexpected error fetching {dataset_key}: {e}")
            return None
            
    def fetch_arrondissement_data(self, arrondissement: str = '14'):
        """Fetch all relevant data for specified arrondissement (extendable design)."""
        print(f"Fetching data for {arrondissement}th arrondissement...")
        
        # Filter for specified arrondissement
        arr_filter = {'arrondissement': arrondissement}
        
        datasets = {}
        successful_fetches = 0
        total_datasets = len(self.DATASETS)
        
        # Priority datasets to fetch first
        priority_datasets = [
            'glass_igloos', 'trilib_stations', 'public_composters', 
            'textile_containers', 'street_bins', 'arrondissement_boundaries'
        ]
        
        # Fetch priority datasets first
        print(f"\nFetching priority datasets...")
        for key in priority_datasets:
            if key in self.DATASETS:
                if key == 'arrondissement_boundaries':
                    df = self.fetch_dataset(key)  # Get all arrondissements
                else:
                    df = self.fetch_dataset(key, arr_filter)
                    
                if df is not None:
                    datasets[key] = df
                    successful_fetches += 1
                    
        # Fetch remaining datasets
        print(f"\nFetching additional datasets...")
        remaining_datasets = [k for k in self.DATASETS.keys() if k not in priority_datasets]
        
        for key in remaining_datasets:
            if key == 'arrondissement_boundaries':
                continue  # Already fetched
            elif key in ['neighborhoods', 'road_network', 'ghg_emissions']:
                # Context datasets - get all data
                df = self.fetch_dataset(key)
            else:
                df = self.fetch_dataset(key, arr_filter)
                
            if df is not None:
                datasets[key] = df
                successful_fetches += 1
        
        print(f"\n📊 Data fetching summary:")
        print(f"   Successfully fetched: {successful_fetches}/{total_datasets} datasets")
        print(f"   Success rate: {(successful_fetches/total_datasets)*100:.1f}%")
        
        return datasets
        
    def fetch_14th_arrondissement_data(self):
        """Fetch data for 14th arrondissement (backward compatibility)."""
        return self.fetch_arrondissement_data('14')
        
    def process_geometric_data(self, df: pd.DataFrame, dataset_name: str) -> gpd.GeoDataFrame:
        """Convert DataFrame with geometry to GeoDataFrame."""
        if 'geometry' not in df.columns:
            print(f"No geometry found for {dataset_name}")
            return None
            
        try:
            # Convert to GeoDataFrame and set geometry column
            gdf = gpd.GeoDataFrame(df)
            
            # Handle different geometry formats
            if df['geometry'].dtype == 'object':
                from shapely.geometry import shape
                gdf['geometry'] = df['geometry'].apply(
                    lambda x: shape(x) if isinstance(x, dict) else x
                )
            
            # Explicitly set the geometry column
            gdf = gdf.set_geometry('geometry')
                
            # Set CRS (Paris uses Lambert 93 - EPSG:2154, but web maps use WGS84)
            gdf = gdf.set_crs('EPSG:4326', allow_override=True)
            
            # Save processed data
            output_file = self.PROCESSED_DATA_DIR / f"{dataset_name}.geojson"
            gdf.to_file(output_file, driver='GeoJSON')
            
            print(f"✓ Saved {len(gdf)} geometric features for {dataset_name}")
            return gdf
            
        except Exception as e:
            print(f"Error processing geometry for {dataset_name}: {e}")
            return None

def main():
    """Main execution function."""
    fetcher = ParisDataFetcher()
    
    print("Starting Paris waste management data collection...")
    print("Focus: 14th arrondissement (Rive Gauche)")
    print("-" * 50)
    
    # Fetch data
    datasets = fetcher.fetch_14th_arrondissement_data()
    
    # Process geometric data
    for name, df in datasets.items():
        if df is not None and 'geometry' in df.columns:
            gdf = fetcher.process_geometric_data(df, name)
            
    print("-" * 50)
    print("Data collection completed!")
    print(f"Raw data saved to: {fetcher.RAW_DATA_DIR}")
    print(f"Processed data saved to: {fetcher.PROCESSED_DATA_DIR}")

if __name__ == "__main__":
    main()
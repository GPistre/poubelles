#!/usr/bin/env python3
"""
Data enrichment script for Paris garbage flow visualization.
Adds missing data through research and extrapolation.
"""

import pandas as pd
import geopandas as gpd
import json
import requests
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np

class DataEnricher:
    """Enriches waste management data with research-based estimates and flow modeling."""
    
    def __init__(self, data_dir: Path = Path("data")):
        self.data_dir = data_dir
        self.processed_dir = data_dir / "processed"
        self.enriched_dir = data_dir / "enriched"
        self.enriched_dir.mkdir(exist_ok=True)
        
    def load_processed_data(self) -> Dict[str, pd.DataFrame]:
        """Load all processed datasets."""
        datasets = {}
        
        for file_path in self.processed_dir.glob("*.geojson"):
            name = file_path.stem
            try:
                gdf = gpd.read_file(file_path)
                datasets[name] = gdf
                print(f"Loaded {name}: {len(gdf)} records")
            except Exception as e:
                print(f"Error loading {name}: {e}")
                
        return datasets
        
    def estimate_waste_flows(self) -> Dict[str, any]:
        """
        Estimate waste flows for 14th arrondissement based on research data.
        Sources: ADEME, Paris waste management reports, EU waste statistics.
        """
        
        # Research-based estimates for Paris 14th arrondissement
        population_14th = 140000  # Approximate population
        
        # Waste generation rates (kg/person/year) - based on ADEME data
        waste_rates = {
            'household_waste': 254,  # Ordures ménagères résiduelles
            'recyclables': 85,       # Emballages et papiers
            'glass': 38,             # Verre
            'organic_waste': 45,     # Bio-déchets (when collected separately)
            'bulky_waste': 25,       # Encombrants
            'electronic_waste': 12   # DEEE
        }
        
        # Calculate annual tonnage for 14th arrondissement
        annual_tonnage = {}
        for waste_type, rate_per_person in waste_rates.items():
            annual_tonnage[waste_type] = (population_14th * rate_per_person) / 1000  # Convert to tonnes
            
        # Collection frequency and flow modeling
        collection_flows = self._model_collection_flows(annual_tonnage)
        
        # Treatment destinations (based on Paris waste management plan)
        treatment_flows = self._model_treatment_flows(annual_tonnage)
        
        return {
            'population': population_14th,
            'annual_tonnage': annual_tonnage,
            'collection_flows': collection_flows,
            'treatment_flows': treatment_flows
        }
        
    def _model_collection_flows(self, annual_tonnage: Dict[str, float]) -> Dict[str, any]:
        """Model collection flows and routes."""
        
        # Collection schedules based on Paris standards
        collection_schedule = {
            'household_waste': {'frequency': 'daily', 'days_per_week': 6},
            'recyclables': {'frequency': 'weekly', 'days_per_week': 1},
            'glass': {'frequency': 'on_demand', 'days_per_week': 0.5},
            'organic_waste': {'frequency': 'twice_weekly', 'days_per_week': 2},
            'bulky_waste': {'frequency': 'on_demand', 'days_per_week': 0.2}
        }
        
        # Calculate daily flows
        daily_flows = {}
        for waste_type, tonnage in annual_tonnage.items():
            if waste_type in collection_schedule:
                schedule = collection_schedule[waste_type]
                weekly_tonnage = tonnage / 52
                if schedule['days_per_week'] > 0:
                    daily_flows[waste_type] = weekly_tonnage / schedule['days_per_week']
                else:
                    daily_flows[waste_type] = 0
                    
        return {
            'daily_flows': daily_flows,
            'collection_schedule': collection_schedule,
            'collection_routes': self._estimate_collection_routes()
        }
        
    def _estimate_collection_routes(self) -> List[Dict]:
        """Estimate collection routes within 14th arrondissement."""
        
        # Major areas in 14th arrondissement for route planning
        neighborhoods = [
            {'name': 'Montparnasse', 'priority': 'high', 'density': 'high'},
            {'name': 'Plaisance', 'priority': 'medium', 'density': 'medium'},
            {'name': 'Petit-Montrouge', 'priority': 'medium', 'density': 'medium'},
            {'name': 'Parc Montsouris', 'priority': 'low', 'density': 'low'}
        ]
        
        routes = []
        for i, neighborhood in enumerate(neighborhoods):
            routes.append({
                'route_id': f"R14_{i+1}",
                'neighborhood': neighborhood['name'],
                'estimated_stops': self._estimate_stops_by_density(neighborhood['density']),
                'estimated_duration_hours': self._estimate_duration_by_density(neighborhood['density']),
                'waste_types': ['household_waste', 'recyclables']
            })
            
        return routes
        
    def _estimate_stops_by_density(self, density: str) -> int:
        """Estimate number of collection stops based on area density."""
        density_stops = {'high': 150, 'medium': 100, 'low': 50}
        return density_stops.get(density, 75)
        
    def _estimate_duration_by_density(self, density: str) -> float:
        """Estimate collection duration based on area density."""
        density_hours = {'high': 8, 'medium': 6, 'low': 4}
        return density_hours.get(density, 6)
        
    def _model_treatment_flows(self, annual_tonnage: Dict[str, float]) -> Dict[str, any]:
        """Model treatment and disposal flows."""
        
        # Treatment facilities serving Paris (research-based)
        treatment_facilities = {
            'incineration': {
                'name': 'Issy-les-Moulineaux',
                'capacity_tonnes_year': 700000,
                'serves_waste_types': ['household_waste'],
                'distance_km': 8,
                'energy_recovery': True
            },
            'recycling_center': {
                'name': 'Centre de tri Nanterre',
                'capacity_tonnes_year': 50000,
                'serves_waste_types': ['recyclables'],
                'distance_km': 15,
                'recovery_rate': 0.85
            },
            'glass_processing': {
                'name': 'Verrerie Brosse',
                'capacity_tonnes_year': 20000,
                'serves_waste_types': ['glass'],
                'distance_km': 25,
                'recovery_rate': 0.95
            },
            'composting': {
                'name': 'Plateforme compostage Gennevilliers',
                'capacity_tonnes_year': 15000,
                'serves_waste_types': ['organic_waste'],
                'distance_km': 18,
                'recovery_rate': 0.75
            }
        }
        
        # Calculate flow destinations
        treatment_flows = {}
        for waste_type, tonnage in annual_tonnage.items():
            for facility_type, facility in treatment_facilities.items():
                if waste_type in facility['serves_waste_types']:
                    treatment_flows[f"{waste_type}_to_{facility_type}"] = {
                        'tonnage': tonnage,
                        'facility': facility['name'],
                        'distance': facility['distance_km'],
                        'transport_emissions_kg_co2': self._calculate_transport_emissions(
                            tonnage, facility['distance_km']
                        )
                    }
                    
        return treatment_flows
        
    def _calculate_transport_emissions(self, tonnage: float, distance_km: float) -> float:
        """Calculate CO2 emissions from waste transport."""
        # Average emission factor for waste collection trucks: 0.8 kg CO2/tonne/km
        emission_factor = 0.8
        return tonnage * distance_km * emission_factor
        
    def create_flow_network(self, datasets: Dict[str, pd.DataFrame]) -> gpd.GeoDataFrame:
        """Create a network representation of waste flows."""
        
        flows = self.estimate_waste_flows()
        
        # Create flow network with nodes and edges
        nodes = []
        edges = []
        
        # Collection points as source nodes
        if 'waste_collection_points' in datasets:
            collection_points = datasets['waste_collection_points']
            for idx, point in collection_points.iterrows():
                nodes.append({
                    'id': f"collection_{idx}",
                    'type': 'collection',
                    'name': point.get('nom', f'Collection Point {idx}'),
                    'geometry': point['geometry'],
                    'daily_capacity_kg': 500  # Estimated
                })
                
        # Treatment facilities as destination nodes
        treatment_destinations = [
            {'name': 'Issy-les-Moulineaux', 'type': 'incineration', 'lat': 48.8247, 'lon': 2.2725},
            {'name': 'Centre de tri Nanterre', 'type': 'recycling', 'lat': 48.8944, 'lon': 2.1981},
            {'name': 'Verrerie Brosse', 'type': 'glass_processing', 'lat': 48.7589, 'lon': 2.3447}
        ]
        
        for dest in treatment_destinations:
            from shapely.geometry import Point
            nodes.append({
                'id': f"treatment_{dest['type']}",
                'type': 'treatment',
                'name': dest['name'],
                'geometry': Point(dest['lon'], dest['lat']),
                'treatment_type': dest['type']
            })
            
        # Create flow edges (simplified)
        for i, collection_node in enumerate([n for n in nodes if n['type'] == 'collection']):
            for treatment_node in [n for n in nodes if n['type'] == 'treatment']:
                edges.append({
                    'source': collection_node['id'],
                    'target': treatment_node['id'],
                    'flow_type': 'waste_transport',
                    'estimated_daily_tonnage': flows['collection_flows']['daily_flows'].get('household_waste', 0) / 10
                })
                
        # Save enriched data
        nodes_gdf = gpd.GeoDataFrame(nodes)
        nodes_gdf.to_file(self.enriched_dir / "flow_nodes.geojson", driver='GeoJSON')
        
        edges_df = pd.DataFrame(edges)
        edges_df.to_json(self.enriched_dir / "flow_edges.json", orient='records', indent=2)
        
        # Save flow estimates
        with open(self.enriched_dir / "waste_flow_estimates.json", 'w') as f:
            json.dump(flows, f, indent=2, default=str)
            
        print(f"Created flow network: {len(nodes)} nodes, {len(edges)} edges")
        return nodes_gdf

def main():
    """Main execution function."""
    enricher = DataEnricher()
    
    print("Starting data enrichment...")
    print("-" * 50)
    
    # Load processed data
    datasets = enricher.load_processed_data()
    
    if not datasets:
        print("No processed data found. Run fetch_paris_data.py first.")
        return
        
    # Create flow network
    flow_network = enricher.create_flow_network(datasets)
    
    print("-" * 50)
    print("Data enrichment completed!")
    print(f"Enriched data saved to: {enricher.enriched_dir}")

if __name__ == "__main__":
    main()
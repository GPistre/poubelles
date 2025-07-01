#!/usr/bin/env python3
"""
Map visualization component for Paris garbage flow application.
Creates interactive maps showing waste flows in the 14th arrondissement.
"""

import folium
import geopandas as gpd
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np
from folium.plugins import HeatMap, MarkerCluster
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

class GarbageFlowVisualizer:
    """Creates interactive maps for garbage flow visualization."""
    
    def __init__(self, data_dir: Path = Path("data")):
        self.data_dir = data_dir
        self.enriched_dir = data_dir / "enriched"
        self.processed_dir = data_dir / "processed"
        self.output_dir = Path("static")
        self.output_dir.mkdir(exist_ok=True)
        
        # Paris 14th arrondissement center coordinates
        self.center_lat = 48.8332
        self.center_lon = 2.3270
        
        # Color schemes for different waste types
        self.waste_colors = {
            'household_waste': '#FF4444',
            'recyclables': '#44FF44',
            'glass': '#4444FF',
            'organic_waste': '#FFA500',
            'bulky_waste': '#800080',
            'electronic_waste': '#008080'
        }
        
        # Collection point colors by type (mapped to valid Folium colors)
        self.collection_colors = {
            'collection': 'red',
            'treatment': 'blue',
            'recycling': 'green',
            'incineration': 'orange',
            'composting': 'darkgreen',
            'glass_igloos': 'blue',
            'trilib_stations': 'green',
            'public_composters': 'darkgreen',
            'textile_containers': 'purple',
            'street_bins': 'orange',
            'recycling_centers': 'cadetblue'
        }
        
        # Icons for different collection point types
        self.collection_icons = {
            'glass_igloos': 'wine-bottle',
            'trilib_stations': 'recycle',
            'public_composters': 'seedling',
            'textile_containers': 'tshirt',
            'street_bins': 'trash',
            'recycling_centers': 'industry',
            'collection': 'trash',
            'treatment': 'industry'
        }
        
    def load_data(self) -> Dict[str, any]:
        """Load all necessary data for visualization."""
        data = {}
        
        try:
            # Load flow network
            if (self.enriched_dir / "flow_nodes.geojson").exists():
                data['nodes'] = gpd.read_file(self.enriched_dir / "flow_nodes.geojson")
                print(f"Loaded {len(data['nodes'])} flow nodes")
                
            if (self.enriched_dir / "flow_edges.json").exists():
                with open(self.enriched_dir / "flow_edges.json") as f:
                    data['edges'] = json.load(f)
                print(f"Loaded {len(data['edges'])} flow edges")
                
            # Load waste flow estimates
            if (self.enriched_dir / "waste_flow_estimates.json").exists():
                with open(self.enriched_dir / "waste_flow_estimates.json") as f:
                    data['flow_estimates'] = json.load(f)
                print("Loaded waste flow estimates")
                
            # Load processed datasets
            for geojson_file in self.processed_dir.glob("*.geojson"):
                name = geojson_file.stem
                try:
                    gdf = gpd.read_file(geojson_file)
                    data[name] = gdf
                    print(f"Loaded {name}: {len(gdf)} features")
                except Exception as e:
                    print(f"Error loading {name}: {e}")
                    
        except Exception as e:
            print(f"Error loading data: {e}")
            
        return data
        
    def create_base_map(self) -> folium.Map:
        """Create base map centered on Paris 14th arrondissement."""
        
        # Create map
        m = folium.Map(
            location=[self.center_lat, self.center_lon],
            zoom_start=14,
            tiles='OpenStreetMap'
        )
        
        # Add additional tile layers
        folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Esri',
            name='Satellite',
            overlay=False,
            control=True
        ).add_to(m)
        
        folium.TileLayer(
            tiles='CartoDB positron',
            name='CartoDB Positron',
            overlay=False,
            control=True
        ).add_to(m)
        
        return m
        
    def add_collection_infrastructure(self, map_obj: folium.Map, data: Dict[str, any]):
        """Add all collection infrastructure to the map with type-specific styling."""
        
        # Collection point types to display
        collection_types = [
            'glass_igloos', 'trilib_stations', 'public_composters', 
            'textile_containers', 'street_bins', 'recycling_centers'
        ]
        
        for collection_type in collection_types:
            if collection_type in data:
                gdf = data[collection_type]
                if gdf is not None and len(gdf) > 0:
                    self._add_collection_type(map_obj, gdf, collection_type)
                    
        # Add legacy collection points if they exist
        if 'nodes' in data:
            self._add_legacy_collection_points(map_obj, data['nodes'])
            
    def _add_collection_type(self, map_obj: folium.Map, gdf: gpd.GeoDataFrame, collection_type: str):
        """Add a specific type of collection points."""
        
        # Create layer group for this collection type
        type_name = collection_type.replace('_', ' ').title()
        cluster = MarkerCluster(name=f"{type_name} ({len(gdf)})").add_to(map_obj)
        
        color = self.collection_colors.get(collection_type, 'gray')
        icon = self.collection_icons.get(collection_type, 'circle')
        
        for idx, point in gdf.iterrows():
            if 'geometry' in point and point.geometry is not None:
                # Extract useful information from the data
                name = point.get('nom', point.get('name', f'{type_name} {idx}'))
                address = point.get('adresse', point.get('address', 'N/A'))
                arrondissement = point.get('arrondissement', point.get('c_ar', 'N/A'))
                
                popup_content = f"""
                <div style="font-family: Arial, sans-serif; width: 250px;">
                    <h4>{name}</h4>
                    <p><strong>Type:</strong> {type_name}</p>
                    <p><strong>Address:</strong> {address}</p>
                    <p><strong>Arrondissement:</strong> {arrondissement}</p>
                </div>
                """
                
                folium.Marker(
                    location=[point.geometry.y, point.geometry.x],
                    popup=folium.Popup(popup_content, max_width=280),
                    tooltip=name,
                    icon=folium.Icon(color=color, icon=icon, prefix='fa')
                ).add_to(cluster)
                
    def _add_legacy_collection_points(self, map_obj: folium.Map, nodes_data: gpd.GeoDataFrame):
        """Add legacy collection points for backward compatibility."""
        
        collection_cluster = MarkerCluster(name="Legacy Collection Points").add_to(map_obj)
        
        for idx, node in nodes_data.iterrows():
            if node['type'] == 'collection':
                popup_content = f"""
                <div style="font-family: Arial, sans-serif; width: 200px;">
                    <h4>{node['name']}</h4>
                    <p><strong>Type:</strong> Collection Point</p>
                    <p><strong>Daily Capacity:</strong> {node.get('daily_capacity_kg', 'N/A')} kg</p>
                </div>
                """
                
                folium.Marker(
                    location=[node.geometry.y, node.geometry.x],
                    popup=folium.Popup(popup_content, max_width=250),
                    tooltip=node['name'],
                    icon=folium.Icon(color='red', icon='trash', prefix='fa')
                ).add_to(collection_cluster)
                
    def add_treatment_facilities(self, map_obj: folium.Map, nodes_data: gpd.GeoDataFrame):
        """Add treatment facilities to the map."""
        
        treatment_cluster = MarkerCluster(name="Treatment Facilities").add_to(map_obj)
        
        for idx, node in nodes_data.iterrows():
            if node['type'] == 'treatment':
                
                # Get color based on treatment type
                treatment_type = node.get('treatment_type', 'unknown')
                color = self.collection_colors.get(treatment_type, 'blue')
                
                popup_content = f"""
                <div style="font-family: Arial, sans-serif; width: 200px;">
                    <h4>{node['name']}</h4>
                    <p><strong>Type:</strong> {treatment_type.replace('_', ' ').title()}</p>
                    <p><strong>Function:</strong> Waste Treatment</p>
                </div>
                """
                
                folium.Marker(
                    location=[node.geometry.y, node.geometry.x],
                    popup=folium.Popup(popup_content, max_width=250),
                    tooltip=node['name'],
                    icon=folium.Icon(color=color, icon='industry', prefix='fa')
                ).add_to(treatment_cluster)
                
    def add_flow_lines(self, map_obj: folium.Map, nodes_data: gpd.GeoDataFrame, edges_data: List[Dict]):
        """Add flow lines between collection and treatment points."""
        
        # Create node lookup for coordinates
        node_coords = {}
        for idx, node in nodes_data.iterrows():
            node_coords[node['id']] = [node.geometry.y, node.geometry.x]
            
        # Add flow lines
        for edge in edges_data:
            source_id = edge['source']
            target_id = edge['target']
            
            if source_id in node_coords and target_id in node_coords:
                source_coords = node_coords[source_id]
                target_coords = node_coords[target_id]
                
                # Line weight based on flow tonnage
                tonnage = edge.get('estimated_daily_tonnage', 0)
                weight = max(2, min(8, tonnage * 2))  # Scale line weight
                
                # Create flow line
                folium.PolyLine(
                    locations=[source_coords, target_coords],
                    color='#FF6B6B',
                    weight=weight,
                    opacity=0.7,
                    popup=f"Daily Flow: {tonnage:.1f} tonnes"
                ).add_to(map_obj)
                
    def add_waste_statistics_overlay(self, map_obj: folium.Map, flow_estimates: Dict):
        """Add waste statistics as an overlay."""
        
        if 'annual_tonnage' not in flow_estimates:
            return
            
        # Create statistics popup
        stats_html = """
        <div style="font-family: Arial, sans-serif; padding: 10px; background: white; border-radius: 5px;">
            <h3>14th Arrondissement Waste Statistics</h3>
            <table style="width: 100%; border-collapse: collapse;">
        """
        
        for waste_type, tonnage in flow_estimates['annual_tonnage'].items():
            color = self.waste_colors.get(waste_type, '#666666')
            stats_html += f"""
                <tr>
                    <td style="padding: 5px;">
                        <span style="color: {color}; font-weight: bold;">●</span> 
                        {waste_type.replace('_', ' ').title()}
                    </td>
                    <td style="padding: 5px; text-align: right;">{tonnage:.0f} tonnes/year</td>
                </tr>
            """
            
        stats_html += """
            </table>
            <p style="margin-top: 10px; font-size: 0.9em; color: #666;">
                Based on ADEME data and research estimates
            </p>
        </div>
        """
        
        # Add statistics marker
        folium.Marker(
            location=[self.center_lat + 0.01, self.center_lon + 0.01],
            popup=folium.Popup(stats_html, max_width=300),
            tooltip="Click for waste statistics",
            icon=folium.Icon(color='green', icon='bar-chart', prefix='fa')
        ).add_to(map_obj)
        
    def add_arrondissement_boundary(self, map_obj: folium.Map, boundary_data: Optional[gpd.GeoDataFrame]):
        """Add 14th arrondissement boundary to the map."""
        
        if boundary_data is None:
            return
            
        # Filter for 14th arrondissement
        arr_14 = boundary_data[boundary_data['c_ar'] == 14] if 'c_ar' in boundary_data.columns else boundary_data
        
        if len(arr_14) > 0:
            # Add boundary outline
            folium.GeoJson(
                arr_14.iloc[0].geometry,
                style_function=lambda x: {
                    'fillColor': 'blue',
                    'color': 'darkblue',
                    'weight': 3,
                    'fillOpacity': 0.1
                },
                tooltip="14th Arrondissement"
            ).add_to(map_obj)
            
    def create_flow_heatmap(self, map_obj: folium.Map, nodes_data: gpd.GeoDataFrame):
        """Create heatmap showing waste collection intensity."""
        
        # Extract collection points
        collection_points = nodes_data[nodes_data['type'] == 'collection']
        
        if len(collection_points) == 0:
            return
            
        # Create heat data
        heat_data = []
        for idx, point in collection_points.iterrows():
            # Use daily capacity as heat intensity
            intensity = point.get('daily_capacity_kg', 500) / 100  # Scale down
            heat_data.append([point.geometry.y, point.geometry.x, intensity])
            
        # Add heatmap
        HeatMap(
            heat_data,
            name="Collection Intensity",
            radius=20,
            blur=15,
            max_zoom=1,
            show=False  # Start hidden
        ).add_to(map_obj)
        
    def create_complete_map(self) -> folium.Map:
        """Create complete interactive map with all layers."""
        
        print("Creating complete garbage flow map...")
        
        # Load data
        data = self.load_data()
        
        if not data:
            print("No data available for visualization")
            return None
            
        # Create base map
        m = self.create_base_map()
        
        # Add layers if data is available
        self.add_collection_infrastructure(m, data)
        
        if 'nodes' in data:
            self.add_treatment_facilities(m, data['nodes'])
            self.create_flow_heatmap(m, data['nodes'])
            
            if 'edges' in data:
                self.add_flow_lines(m, data['nodes'], data['edges'])
                
        if 'flow_estimates' in data:
            self.add_waste_statistics_overlay(m, data['flow_estimates'])
            
        if 'arrondissement_boundaries' in data:
            self.add_arrondissement_boundary(m, data['arrondissement_boundaries'])
            
        # Add layer control
        folium.LayerControl().add_to(m)
        
        return m
        
    def save_map(self, map_obj: folium.Map, filename: str = "garbage_flow_map.html"):
        """Save map to HTML file."""
        
        output_path = self.output_dir / filename
        map_obj.save(str(output_path))
        print(f"Map saved to: {output_path}")
        return output_path

def main():
    """Main execution function."""
    visualizer = GarbageFlowVisualizer()
    
    print("Creating Paris garbage flow visualization...")
    print("Focus: 14th arrondissement")
    print("-" * 50)
    
    # Create complete map
    map_obj = visualizer.create_complete_map()
    
    if map_obj:
        # Save map
        output_path = visualizer.save_map(map_obj)
        
        print("-" * 50)
        print("Visualization completed!")
        print(f"Open {output_path} in your browser to view the map")
    else:
        print("Failed to create map. Check if data files exist.")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Main execution script for Paris garbage flow visualization.
Coordinates data fetching, enrichment, and visualization.
"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scripts.fetch_paris_data import ParisDataFetcher
from scripts.enrich_data import DataEnricher
from src.map_visualizer import GarbageFlowVisualizer

def run_full_pipeline(arrondissement: str = '14'):
    """Run the complete data pipeline."""
    print("=" * 60)
    print("PARIS GARBAGE FLOW VISUALIZATION")
    print(f"{arrondissement}th Arrondissement")
    print("=" * 60)
    
    # Step 1: Fetch data
    print(f"\n1. Fetching Paris open data for {arrondissement}th arrondissement...")
    fetcher = ParisDataFetcher()
    datasets = fetcher.fetch_arrondissement_data(arrondissement)
    
    # Process geometric data
    for name, df in datasets.items():
        if df is not None and 'geometry' in df.columns:
            fetcher.process_geometric_data(df, name)
    
    # Step 2: Enrich data
    print("\n2. Enriching data with research estimates...")
    enricher = DataEnricher()
    enriched_datasets = enricher.load_processed_data()
    
    if enriched_datasets:
        enricher.create_flow_network(enriched_datasets)
    else:
        print("Warning: No processed data found for enrichment")
    
    # Step 3: Create visualization
    print("\n3. Creating interactive map...")
    visualizer = GarbageFlowVisualizer()
    map_obj = visualizer.create_complete_map()
    
    if map_obj:
        output_path = visualizer.save_map(map_obj)
        print(f"\n✅ SUCCESS: Map created at {output_path}")
        print("\nTo view the map:")
        print(f"  open {output_path}")
        print("  or")
        print(f"  python -m http.server 8000")
        print("  then go to http://localhost:8000/static/garbage_flow_map.html")
    else:
        print("\n❌ ERROR: Failed to create map")
        return False
        
    return True

def run_individual_step(step: str):
    """Run an individual pipeline step."""
    
    if step == "fetch":
        print("Fetching Paris open data...")
        fetcher = ParisDataFetcher()
        datasets = fetcher.fetch_14th_arrondissement_data()
        
        for name, df in datasets.items():
            if df is not None and 'geometry' in df.columns:
                fetcher.process_geometric_data(df, name)
                
    elif step == "enrich":
        print("Enriching data...")
        enricher = DataEnricher()
        datasets = enricher.load_processed_data()
        
        if datasets:
            enricher.create_flow_network(datasets)
        else:
            print("No processed data found. Run 'fetch' step first.")
            
    elif step == "visualize":
        print("Creating visualization...")
        visualizer = GarbageFlowVisualizer()
        map_obj = visualizer.create_complete_map()
        
        if map_obj:
            output_path = visualizer.save_map(map_obj)
            print(f"Map saved to: {output_path}")
        else:
            print("Failed to create map")
            
    else:
        print(f"Unknown step: {step}")
        print("Available steps: fetch, enrich, visualize")

def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(
        description="Paris Garbage Flow Visualization for 14th Arrondissement"
    )
    
    parser.add_argument(
        '--step',
        choices=['fetch', 'enrich', 'visualize', 'all'],
        default='all',
        help='Pipeline step to run (default: all)'
    )
    
    parser.add_argument(
        '--arrondissement',
        default='14',
        help='Paris arrondissement to analyze (default: 14)'
    )
    
    parser.add_argument(
        '--serve',
        action='store_true',
        help='Start local web server to view the map'
    )
    
    args = parser.parse_args()
    
    if args.step == 'all':
        success = run_full_pipeline(args.arrondissement)
        if not success:
            sys.exit(1)
    else:
        run_individual_step(args.step)
    
    if args.serve:
        import http.server
        import socketserver
        import webbrowser
        import os
        
        PORT = 8000
        
        # Change to project directory
        os.chdir(Path(__file__).parent)
        
        Handler = http.server.SimpleHTTPRequestHandler
        
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            print(f"\nStarting web server at http://localhost:{PORT}")
            print("Open http://localhost:{PORT}/static/garbage_flow_map.html to view the map")
            print("Press Ctrl+C to stop the server")
            
            # Try to open browser automatically
            try:
                webbrowser.open(f"http://localhost:{PORT}/static/garbage_flow_map.html")
            except:
                pass
                
            httpd.serve_forever()

if __name__ == "__main__":
    main()
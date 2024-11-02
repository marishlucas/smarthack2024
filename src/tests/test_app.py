# tests/test_app.py

import os
import sys
import pandas as pd
import logging
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

from data_loader import DataLoader
from api_client import APIClient
from models import Node, Connection, Demand
from optimizer import Optimizer


def setup_test_data():
    """Create test data directory and files"""
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    # Create test CSV files
    connections_data = """id;from_id;to_id;distance;lead_time_days;connection_type;max_capacity
79a7eaac-482a-4cd6-a5ee-596165f47f01;21b22968-9ef1-4568-bdd3-29d4488191ed;37572640-6ac6-4927-af72-4602d9d9c3b0;607;5;TRUCK;158"""

    customers_data = """id;name;max_input;over_input_penalty;late_delivery_penalty;early_delivery_penalty;node_type
169a8ef4-2abf-44a2-9aa1-87286403945c;customer 1;260;4.33;0.24;0.62;CUSTOMER"""

    demands_data = """id;customer_id;quantity;post_day;start_delivery_day;end_delivery_day
26d14692-f7e9-421c-8778-19e83bbe9b7b;6e4bd881-0860-4a49-aa89-0d3d3d051c25;786;0;6;21"""

    refineries_data = """id;name;capacity;max_output;production;overflow_penalty;underflow_penalty;over_output_penalty;production_cost;production_co2;initial_stock;node_type
beb6ba68-6d89-48e0-a6aa-1ee978bafa27;refinery 0;1870;223;186;5.04;2.03;3.23;3.74;5.16;55;REFINERY"""

    tanks_data = """id;name;capacity;max_input;max_output;overflow_penalty;underflow_penalty;over_input_penalty;over_output_penalty;initial_stock;node_type
21b22968-9ef1-4568-bdd3-29d4488191ed;tank 0;404476;2895;11063;5.35;2.88;4.1;2.81;342565;STORAGE_TANK"""

    teams_data = """id;color;name;api_key;internal_use
99f0c049-7c71-47c7-860c-aa128e49cfe;#00b9f2;SmartHacks202;7bcd6334-bc2e-4cbf-b9d4-61cb9e868869;false"""

    files = {
        "connections.csv": connections_data,
        "customers.csv": customers_data,
        "demands.csv": demands_data,
        "refineries.csv": refineries_data,
        "tanks.csv": tanks_data,
        "teams.csv": teams_data,
    }

    for filename, data in files.items():
        with open(data_dir / filename, "w", encoding="utf-8") as f:
            f.write(data)


def test_data_loader():
    """Test the DataLoader class"""
    print("\nTesting DataLoader...")
    loader = DataLoader()

    try:
        refineries = loader.load_refineries()
        tanks = loader.load_tanks()
        customers = loader.load_customers()
        connections = loader.load_connections()
        demands = loader.load_demands()
        teams = loader.load_teams()

        print("✓ All data files loaded successfully")
        return True
    except Exception as e:
        print(f"✗ Error loading data: {str(e)}")
        return False


def test_api_client():
    """Test the APIClient class"""
    print("\nTesting APIClient...")
    api_key = "7bcd6334-bc2e-4cbf-b9d4-61cb9e868869"
    client = APIClient(api_key)

    try:
        # Test session start
        success = client.start_session()
        if not success:
            print("✗ Failed to start session")
            return False
        print("✓ Session started successfully")

        # Test playing a round
        response = client.play_round(1, [])
        if response is None:
            print("✗ Failed to play round")
            return False
        print("✓ Round played successfully")

        # Test session end
        success = client.end_session()
        if not success:
            print("✗ Failed to end session")
            return False
        print("✓ Session ended successfully")

        return True
    except Exception as e:
        print(f"✗ Error testing API client: {str(e)}")
        return False


def test_optimizer():
    """Test the Optimizer class"""
    print("\nTesting Optimizer...")
    try:
        # Create test nodes
        nodes = {
            "ref1": Node(
                id="ref1",
                type="refinery",
                capacity=1000.0,
                daily_output=100.0,
                daily_input=0.0,
                stock=500.0,
            ),
            "tank1": Node(
                id="tank1",
                type="tank",
                capacity=2000.0,
                daily_output=200.0,
                daily_input=200.0,
                stock=1000.0,
            ),
            "cust1": Node(
                id="cust1",
                type="customer",
                capacity=0.0,
                daily_output=0.0,
                daily_input=100.0,
                stock=0.0,
            ),
        }

        # Create test connections
        connections = {
            "conn1": Connection(
                id="conn1",
                source="ref1",
                destination="tank1",
                distance=100.0,
                lead_time_days=1,
                connection_type="pipeline",
                max_capacity=500.0,
                cost_per_unit=0.5,
                co2_per_unit=0.2,
            )
        }

        # Create test demands
        demands = [
            Demand(
                id="dem1",
                customer_id="cust1",
                quantity=100.0,
                post_day=1,
                start_delivery_day=2,
                end_delivery_day=5,
            )
        ]

        optimizer = Optimizer(nodes, connections, demands, current_day=1)
        movements = optimizer.optimize()

        print("✓ Optimizer ran successfully")
        print(f"  Generated {len(movements)} movements")
        return True
    except Exception as e:
        print(f"✗ Error testing optimizer: {str(e)}")
        return False


def main():
    """Main test function"""
    print("Starting tests...")

    # Setup test data
    try:
        setup_test_data()
        print("✓ Test data setup complete")
    except Exception as e:
        print(f"✗ Failed to setup test data: {str(e)}")
        return

    # Run tests
    tests = [test_data_loader, test_api_client, test_optimizer]

    success = all(test() for test in tests)

    if success:
        print("\n✓ All tests passed successfully!")
    else:
        print("\n✗ Some tests failed. Check the output above for details.")


if __name__ == "__main__":
    main()

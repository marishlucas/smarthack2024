# src/main.py

import logging
from pathlib import Path
import sys
from typing import Dict, List, Optional
from datetime import datetime

from data_loader import DataLoader
from api_client import APIClient
from models import Node, Connection, Demand, CONNECTION_TYPE_MAPPING
from optimizer import Optimizer


def setup_logging():
    """Configure logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"fuel_optimization_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)],
    )
    logging.info(f"Starting new optimization run at {timestamp}")


def check_required_columns(df, required_columns: set, file_name: str):
    """Verify that all required columns are present in the DataFrame"""
    actual_columns = set(df.columns.str.strip().str.lower())
    missing = required_columns - actual_columns
    if missing:
        logging.error(f"Missing columns in {file_name}: {missing}")
        sys.exit(1)
    else:
        logging.info(f"All required columns in {file_name} are present.")


def main():
    """Main application entry point"""
    try:
        # Setup logging
        setup_logging()

        # Initialize data loader
        data_loader = DataLoader()

        # Load and validate data files
        try:
            refineries_df = data_loader.load_refineries()
            tanks_df = data_loader.load_tanks()
            customers_df = data_loader.load_customers()
            connections_df = data_loader.load_connections()
            demands_df = data_loader.load_demands()

            # Define required columns for each file
            required_refinery_columns = {
                "id",
                "name",
                "capacity",
                "max_output",
                "production",
                "overflow_penalty",
                "underflow_penalty",
                "over_output_penalty",
                "production_cost",
                "production_co2",
                "initial_stock",
                "node_type",
            }

            required_tank_columns = {
                "id",
                "name",
                "capacity",
                "max_input",
                "max_output",
                "overflow_penalty",
                "underflow_penalty",
                "over_input_penalty",
                "over_output_penalty",
                "initial_stock",
                "node_type",
            }

            required_customer_columns = {
                "id",
                "name",
                "max_input",
                "over_input_penalty",
                "late_delivery_penalty",
                "early_delivery_penalty",
                "node_type",
            }

            required_connection_columns = {
                "id",
                "from_id",
                "to_id",
                "distance",
                "lead_time_days",
                "connection_type",
                "max_capacity",
            }

            required_demand_columns = {
                "id",
                "customer_id",
                "quantity",
                "post_day",
                "start_delivery_day",
                "end_delivery_day",
            }

            # Check all required columns
            check_required_columns(
                refineries_df, required_refinery_columns, "refineries.csv"
            )
            check_required_columns(tanks_df, required_tank_columns, "tanks.csv")
            check_required_columns(
                customers_df, required_customer_columns, "customers.csv"
            )
            check_required_columns(
                connections_df, required_connection_columns, "connections.csv"
            )
            check_required_columns(demands_df, required_demand_columns, "demands.csv")

        except FileNotFoundError as e:
            logging.error(f"Data file not found: {e}")
            sys.exit(1)
        except pd.errors.EmptyDataError as e:
            logging.error(f"Data file is empty or corrupted: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"An error occurred while loading data: {e}")
            sys.exit(1)

        # Create nodes dictionary
        nodes = {}

        # Process refineries
        for _, row in refineries_df.iterrows():
            try:
                node = Node(
                    id=str(row["id"]),
                    type="refinery",
                    capacity=float(row["capacity"]),
                    daily_output=float(row["max_output"]),
                    daily_input=0.0,
                    stock=float(row["initial_stock"]),
                )
                nodes[node.id] = node
            except (KeyError, ValueError) as e:
                logging.error(f"Error processing refinery data: {e}")
                sys.exit(1)

        # Process tanks
        for _, row in tanks_df.iterrows():
            try:
                node = Node(
                    id=str(row["id"]),
                    type="tank",
                    capacity=float(row["capacity"]),
                    daily_output=float(row["max_output"]),
                    daily_input=float(row["max_input"]),
                    stock=float(row["initial_stock"]),
                )
                nodes[node.id] = node
            except (KeyError, ValueError) as e:
                logging.error(f"Error processing tank data: {e}")
                sys.exit(1)

        # Process customers
        for _, row in customers_df.iterrows():
            try:
                node = Node(
                    id=str(row["id"]),
                    type="customer",
                    capacity=0.0,
                    daily_output=0.0,
                    daily_input=float(row["max_input"]),
                    stock=0.0,
                )
                nodes[node.id] = node
            except (KeyError, ValueError) as e:
                logging.error(f"Error processing customer data: {e}")
                sys.exit(1)

        # Create connections dictionary
        connections = {}
        for _, row in connections_df.iterrows():
            try:
                connection_type = row["connection_type"].strip().lower()
                type_info = CONNECTION_TYPE_MAPPING.get(
                    connection_type, {"cost_per_unit": 1.0, "co2_per_unit": 0.5}
                )

                conn = Connection(
                    id=str(row["id"]),
                    source=str(row["from_id"]),
                    destination=str(row["to_id"]),
                    distance=float(row["distance"]),
                    lead_time_days=int(row["lead_time_days"]),
                    connection_type=connection_type,
                    max_capacity=float(row["max_capacity"]),
                    cost_per_unit=type_info["cost_per_unit"],
                    co2_per_unit=type_info["co2_per_unit"],
                )
                connections[conn.id] = conn
            except (KeyError, ValueError) as e:
                logging.error(f"Error processing connection data: {e}")
                sys.exit(1)

        # Initialize API client
        api_key = "7bcd6334-bc2e-4cbf-b9d4-61cb9e868869"
        api_client = APIClient(api_key)

        # Start session
        if not api_client.start_session():
            logging.error("Failed to start session")
            sys.exit(1)

        # Initialize tracking variables
        demands: List[Demand] = []
        shipments_in_transit = {}

        # Main game loop - 42 rounds starting from day 0
        for current_day in range(43):  # 0 to 42 inclusive
            logging.info(f"\n{'='*20} Day {current_day} {'='*20}")

            # Process arriving shipments
            if current_day in shipments_in_transit:
                for shipment in shipments_in_transit[current_day]:
                    node_id = shipment["toNode"]
                    amount = shipment["amount"]
                    if node_id in nodes:
                        nodes[node_id].stock += amount
                        logging.info(
                            f"Shipment arrived: {amount:.2f} units at {node_id}"
                        )
                del shipments_in_transit[current_day]

            # Day 0 gets empty movements
            if current_day == 0:
                movements = []
            else:
                # Update refinery production
                for node_id, node in nodes.items():
                    if node.type == "refinery":
                        production_rate = next(
                            float(row["production"])
                            for _, row in refineries_df.iterrows()
                            if str(row["id"]) == node_id
                        )
                        node.stock += production_rate
                        logging.info(
                            f"Refinery {node_id} produced {production_rate:.2f} units"
                        )

                # Run optimization
                optimizer = Optimizer(
                    nodes=nodes,
                    connections=connections,
                    demands=[d for d in demands if d.remaining_amount > 0],
                    current_day=current_day,
                )
                movements = optimizer.optimize()

            # Submit movements to API
            day_response = api_client.play_round(current_day, movements)
            if day_response is None:
                logging.error(f"Failed to process day {current_day}")
                break

            # Process new demands from API response
            new_demands = day_response.get("demand", [])
            for demand_data in new_demands:
                try:
                    demand = Demand(
                        id=str(demand_data["id"]),
                        customer_id=str(demand_data["customer_id"]),
                        quantity=float(demand_data["quantity"]),
                        post_day=int(demand_data["post_day"]),
                        start_delivery_day=int(demand_data["start_delivery_day"]),
                        end_delivery_day=int(demand_data["end_delivery_day"]),
                    )
                    demands.append(demand)
                    logging.info(
                        f"New demand: Customer={demand.customer_id}, "
                        f"Quantity={demand.quantity}, "
                        f"Window={demand.start_delivery_day}-{demand.end_delivery_day}"
                    )
                except (KeyError, ValueError) as e:
                    logging.error(f"Error processing demand data: {e}")

            # Process movements and schedule future arrivals
            for movement in movements:
                from_node_id = movement["fromNode"]
                to_node_id = movement["toNode"]
                amount = movement["amount"]
                lead_time = movement["leadTime"]
                arrival_day = current_day + lead_time

                if from_node_id in nodes:
                    nodes[from_node_id].stock -= amount

                    if arrival_day not in shipments_in_transit:
                        shipments_in_transit[arrival_day] = []
                    shipments_in_transit[arrival_day].append(
                        {"toNode": to_node_id, "amount": amount}
                    )

                    logging.info(
                        f"Movement scheduled: {amount:.2f} units "
                        f"from {from_node_id} to {to_node_id}, "
                        f"arriving day {arrival_day}"
                    )

            # Log daily stats
            penalties = day_response.get("penalties", [])
            if penalties:
                logging.warning(f"Day {current_day} Penalties:")
                for penalty in penalties:
                    logging.warning(f"  {penalty['type']}: {penalty['message']}")

            if "deltaKpis" in day_response:
                kpis = day_response["deltaKpis"]
                logging.info(
                    f"Day {current_day} KPIs - "
                    f"Cost: {kpis['cost']:.2f}, CO2: {kpis['co2']:.2f}"
                )

        # End session
        if not api_client.end_session():
            logging.error("Failed to end session")

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        try:
            api_client.end_session()
        except:
            pass
        raise


if __name__ == "__main__":
    main()


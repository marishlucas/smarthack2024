from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus, PULP_CBC_CMD
from typing import Dict, List, Optional
import logging
from models import Node, Connection, Demand


class Optimizer:
    def __init__(
        self,
        nodes: Dict[str, Node],
        connections: Dict[str, Connection],
        demands: List[Demand],
        current_day: int,
        planning_horizon: int = 7,
        total_days: int = 42,
    ):
        self.nodes = nodes
        self.connections = connections
        self.demands = demands
        self.current_day = current_day
        self.total_days = total_days
        self.planning_horizon = min(planning_horizon, total_days - current_day + 1)

        # Track if we're in end-game phase
        self.is_endgame = (total_days - current_day) <= 5

        # Initialize maps for tracking connections and stocks
        self.refinery_routes = self._find_refinery_routes()
        self.tank_routes = self._find_tank_routes()
        self.customer_routes = self._find_customer_routes()
        self.projected_stocks = {}

        # Configure weights based on game phase
        if self.is_endgame:
            self.cost_weight = 0.1  # Lower cost importance
            self.co2_weight = 0.1  # Lower CO2 importance
            self.demand_weight = 10.0  # Higher demand priority
            self.overflow_weight = 50.0  # Very high overflow prevention
        else:
            self.cost_weight = 1.0
            self.co2_weight = 0.5
            self.demand_weight = 2.0
            self.overflow_weight = 5.0

        # Setup logging
        self.logger = logging.getLogger(__name__)

    def _find_refinery_routes(self) -> Dict[str, List[Dict]]:
        """Find all valid routes from refineries"""
        routes = {}
        for node_id, node in self.nodes.items():
            if node.type == "refinery":
                routes[node_id] = []
                for conn_id, conn in self.connections.items():
                    if conn.source == node_id:
                        routes[node_id].append(
                            {
                                "conn_id": conn_id,
                                "dest_id": conn.destination,
                                "lead_time": conn.lead_time_days,
                                "max_capacity": conn.max_capacity,
                            }
                        )
        return routes

    def _find_tank_routes(self) -> Dict[str, List[Dict]]:
        """Find all valid routes from storage tanks"""
        routes = {}
        for node_id, node in self.nodes.items():
            if node.type == "tank":
                routes[node_id] = []
                for conn_id, conn in self.connections.items():
                    if conn.source == node_id:
                        routes[node_id].append(
                            {
                                "conn_id": conn_id,
                                "dest_id": conn.destination,
                                "lead_time": conn.lead_time_days,
                                "max_capacity": conn.max_capacity,
                            }
                        )
        return routes

    def _find_customer_routes(self) -> Dict[str, List[Dict]]:
        """Find all valid routes to customers"""
        routes = {}
        for node_id, node in self.nodes.items():
            if node.type == "customer":
                routes[node_id] = []
                for conn_id, conn in self.connections.items():
                    if conn.destination == node_id:
                        routes[node_id].append(
                            {
                                "conn_id": conn_id,
                                "source_id": conn.source,
                                "lead_time": conn.lead_time_days,
                                "max_capacity": conn.max_capacity,
                            }
                        )
        return routes

    def optimize(self) -> List[Dict]:
        """Main optimization method"""
        try:
            # First handle any critical refinery situations
            movements = self._handle_critical_refineries()

            # Then handle regular optimization
            if not self.is_endgame:
                movements.extend(self._optimize_normal())
            else:
                movements.extend(self._optimize_endgame())

            return movements

        except Exception as e:
            self.logger.error(f"Optimization error: {str(e)}")
            return []

    def _handle_critical_refineries(self) -> List[Dict]:
        """Handle refineries that are close to overflow"""
        movements = []

        for refinery_id, node in self.nodes.items():
            if node.type != "refinery":
                continue

            # Calculate current capacity usage and projection
            capacity_used_percent = (node.stock / node.capacity) * 100
            projected_stock = node.stock + node.daily_output

            # Check if refinery needs urgent clearing
            if capacity_used_percent > 70 or projected_stock > node.capacity * 0.9:
                self.logger.info(
                    f"Refinery {refinery_id} at {capacity_used_percent:.1f}% capacity"
                )

                # Get available routes sorted by lead time
                routes = sorted(
                    self.refinery_routes.get(refinery_id, []),
                    key=lambda x: x["lead_time"],
                )

                remaining_stock = node.stock
                for route in routes:
                    if remaining_stock <= 0:
                        break

                    dest_node = self.nodes[route["dest_id"]]

                    # Calculate available space in destination
                    if dest_node.type == "tank":
                        available_space = dest_node.capacity - dest_node.stock
                        # Consider projected incoming stock
                        arrival_day = self.current_day + route["lead_time"]
                        if arrival_day in self.projected_stocks:
                            if route["dest_id"] in self.projected_stocks[arrival_day]:
                                available_space -= self.projected_stocks[arrival_day][
                                    route["dest_id"]
                                ]
                    else:  # customer
                        available_space = dest_node.daily_input

                    # Calculate maximum possible transfer
                    amount = min(
                        remaining_stock,
                        route["max_capacity"],
                        available_space,
                        node.daily_output,
                    )

                    if amount > 0:
                        movement = {
                            "connectionId": route["conn_id"],
                            "amount": float(amount),
                            "fromNode": refinery_id,
                            "toNode": route["dest_id"],
                            "postedDay": self.current_day,
                            "leadTime": route["lead_time"],
                        }
                        movements.append(movement)
                        remaining_stock -= amount

                        # Update projected stocks
                        arrival_day = self.current_day + route["lead_time"]
                        if arrival_day not in self.projected_stocks:
                            self.projected_stocks[arrival_day] = {}
                        if route["dest_id"] not in self.projected_stocks[arrival_day]:
                            self.projected_stocks[arrival_day][route["dest_id"]] = 0
                        self.projected_stocks[arrival_day][route["dest_id"]] += amount

                        self.logger.info(
                            f"Scheduled movement of {amount:.1f} units from "
                            f"refinery {refinery_id} to {route['dest_id']}"
                        )

        return movements

    def _optimize_normal(self) -> List[Dict]:
        """Regular optimization phase"""
        model = LpProblem("Fuel_Supply_Chain", LpMinimize)

        # Create flow variables
        flow_vars = self._create_flow_variables()

        # Build objective function
        obj_function = self._build_objective_function(flow_vars)
        model += obj_function

        # Add constraints
        self._add_capacity_constraints(model, flow_vars)
        self._add_flow_conservation_constraints(model, flow_vars)
        self._add_demand_fulfillment_constraints(model, flow_vars)

        # Solve model
        solver = PULP_CBC_CMD(msg=False)
        status = model.solve(solver)

        if LpStatus[status] != "Optimal":
            self.logger.warning(f"Non-optimal solution status: {LpStatus[status]}")
            return []

        return self._extract_movements(flow_vars)

    def _optimize_endgame(self) -> List[Dict]:
        """End-game optimization phase"""
        movements = []

        # First priority: Clear refineries completely
        for refinery_id, node in self.nodes.items():
            if node.type != "refinery" or node.stock <= 0:
                continue

            routes = sorted(
                self.refinery_routes.get(refinery_id, []), key=lambda x: x["lead_time"]
            )

            remaining_stock = node.stock
            for route in routes:
                if remaining_stock <= 0:
                    break

                # Only consider movements that will complete before game end
                if self.current_day + route["lead_time"] > self.total_days:
                    continue

                dest_node = self.nodes[route["dest_id"]]
                available_space = (
                    dest_node.capacity - dest_node.stock
                    if dest_node.type == "tank"
                    else dest_node.daily_input
                )

                amount = min(
                    remaining_stock,
                    route["max_capacity"],
                    available_space,
                    node.daily_output,
                )

                if amount > 0:
                    movement = {
                        "connectionId": route["conn_id"],
                        "amount": float(amount),
                        "fromNode": refinery_id,
                        "toNode": route["dest_id"],
                        "postedDay": self.current_day,
                        "leadTime": route["lead_time"],
                    }
                    movements.append(movement)
                    remaining_stock -= amount

        # Second priority: Fulfill remaining demands
        for demand in self.demands:
            if demand.remaining_amount <= 0:
                continue

            routes = self.customer_routes.get(demand.customer_id, [])
            routes = [
                r
                for r in routes
                if self.current_day + r["lead_time"] <= self.total_days
            ]

            if not routes:
                continue

            remaining_amount = demand.remaining_amount
            for route in sorted(routes, key=lambda x: x["lead_time"]):
                if remaining_amount <= 0:
                    break

                source_node = self.nodes[route["source_id"]]
                if source_node.stock <= 0:
                    continue

                amount = min(
                    remaining_amount,
                    route["max_capacity"],
                    source_node.stock,
                    source_node.daily_output
                    if source_node.type == "refinery"
                    else source_node.daily_output,
                )

                if amount > 0:
                    movement = {
                        "connectionId": route["conn_id"],
                        "amount": float(amount),
                        "fromNode": route["source_id"],
                        "toNode": demand.customer_id,
                        "postedDay": self.current_day,
                        "leadTime": route["lead_time"],
                    }
                    movements.append(movement)
                    remaining_amount -= amount
                    source_node.stock -= amount

        return movements

    def _create_flow_variables(self) -> Dict:
        """Create flow variables for optimization"""
        flow_vars = {}
        for conn_id, conn in self.connections.items():
            for day in range(
                self.current_day, self.current_day + self.planning_horizon
            ):
                var_name = f"flow_{conn_id}_day_{day}"

                # Calculate appropriate upper bound
                source_node = self.nodes[conn.source]
                dest_node = self.nodes[conn.destination]

                max_outflow = min(
                    source_node.daily_output,
                    source_node.stock
                    if source_node.type != "refinery"
                    else float("inf"),
                )

                max_inflow = (
                    dest_node.daily_input
                    if dest_node.type == "customer"
                    else dest_node.capacity - dest_node.stock
                )

                upper_bound = min(conn.max_capacity, max_outflow, max_inflow)

                flow_vars[(conn_id, day)] = LpVariable(
                    var_name, lowBound=0, upBound=upper_bound
                )

        return flow_vars

    def _build_objective_function(self, flow_vars):
        """Build the complete objective function"""
        transport_costs = lpSum(
            [
                flow_vars[(conn_id, day)]
                * self.connections[conn_id].cost_per_unit
                * self.connections[conn_id].distance
                for (conn_id, day) in flow_vars.keys()
            ]
        )

        co2_emissions = lpSum(
            [
                flow_vars[(conn_id, day)]
                * self.connections[conn_id].co2_per_unit
                * self.connections[conn_id].distance
                for (conn_id, day) in flow_vars.keys()
            ]
        )

        overflow_prevention = lpSum(
            [
                max(0, node.stock - node.capacity * 0.8) * self.overflow_weight
                for node in self.nodes.values()
                if node.type in ["refinery", "tank"]
            ]
        )

        return (
            self.cost_weight * transport_costs
            + self.co2_weight * co2_emissions
            + overflow_prevention
        )

    def _add_capacity_constraints(self, model, flow_vars):
        """Add all capacity-related constraints"""
        for node_id, node in self.nodes.items():
            for day in range(
                self.current_day, self.current_day + self.planning_horizon
            ):
                # Calculate expected stock level
                inflow = self._calculate_inflow(node_id, day, flow_vars)
                outflow = self._calculate_outflow(node_id, day, flow_vars)

                if node.type == "refinery":
                    model += (
                        node.stock + inflow - outflow + node.daily_output
                        <= node.capacity
                    )
                elif node.type == "tank":
                    model += node.stock + inflow - outflow <= node.capacity
                    model += inflow <= node.daily_input

                if node.type in ["refinery", "tank"]:
                    model += outflow <= node.daily_output
                else:  # customer
                    model += inflow <= node.daily_input

    def _add_flow_conservation_constraints(self, model, flow_vars):
        """Add flow conservation constraints"""
        for node_id, node in self.nodes.items():
            for day in range(
                self.current_day, self.current_day + self.planning_horizon
            ):
                inflow = self._calculate_inflow(node_id, day, flow_vars)
                outflow = self._calculate_outflow(node_id, day, flow_vars)

                # Ensure stock stays non-negative
                if node.type != "customer":
                    stock = node.stock + inflow - outflow
                    if node.type == "refinery":
                        stock += node.daily_output
                    model += stock >= 0

                    # Add strong flow conservation for tanks
                    if node.type == "tank":
                        model += inflow <= node.capacity - node.stock

    def _add_demand_fulfillment_constraints(self, model, flow_vars):
        """Add demand fulfillment constraints"""
        for demand in self.demands:
            if demand.remaining_amount <= 0:
                continue

            # Only consider demands that can be fulfilled within planning horizon
            if demand.end_delivery_day >= self.current_day:
                delivery_window = range(
                    max(self.current_day, demand.start_delivery_day),
                    min(
                        self.current_day + self.planning_horizon,
                        demand.end_delivery_day + 1,
                    ),
                )

                if not delivery_window:
                    continue

                # Calculate total delivery for this demand
                total_delivery = lpSum(
                    [
                        flow_vars.get(
                            (conn_id, day - self.connections[conn_id].lead_time_days), 0
                        )
                        for day in delivery_window
                        for conn_id, conn in self.connections.items()
                        if conn.destination == demand.customer_id
                        and (conn_id, day - conn.lead_time_days) in flow_vars
                    ]
                )

                # Force minimum delivery based on urgency
                days_until_due = demand.end_delivery_day - self.current_day
                if days_until_due <= 3:  # Urgent demand
                    model += total_delivery >= min(
                        demand.remaining_amount * 0.5,
                        self.nodes[demand.customer_id].daily_input
                        * len(delivery_window),
                    )
                else:
                    model += total_delivery >= min(
                        demand.remaining_amount * 0.3,
                        self.nodes[demand.customer_id].daily_input
                        * len(delivery_window),
                    )

    def _calculate_inflow(self, node_id: str, day: int, flow_vars) -> lpSum:
        """Calculate total inflow for a node"""
        base_inflow = lpSum(
            [
                flow_vars.get((conn_id, day - conn.lead_time_days), 0)
                for conn_id, conn in self.connections.items()
                if conn.destination == node_id
                and day - conn.lead_time_days >= self.current_day
            ]
        )

        # Add projected inflows
        if day in self.projected_stocks and node_id in self.projected_stocks[day]:
            return base_inflow + self.projected_stocks[day][node_id]
        return base_inflow

    def _calculate_outflow(self, node_id: str, day: int, flow_vars) -> lpSum:
        """Calculate total outflow for a node"""
        return lpSum(
            [
                flow_vars.get((conn_id, day), 0)
                for conn_id, conn in self.connections.items()
                if conn.source == node_id
            ]
        )

    def _extract_movements(self, flow_vars) -> List[Dict]:
        """Extract actual movements from optimization results"""
        movements = []

        for (conn_id, day), var in flow_vars.items():
            if day == self.current_day and var.varValue > 0:
                conn = self.connections[conn_id]
                movement = {
                    "connectionId": conn_id,
                    "amount": float(var.varValue),
                    "fromNode": conn.source,
                    "toNode": conn.destination,
                    "postedDay": self.current_day,
                    "leadTime": conn.lead_time_days,
                }
                movements.append(movement)

                # Track this movement in projected stocks
                arrival_day = self.current_day + conn.lead_time_days
                if arrival_day not in self.projected_stocks:
                    self.projected_stocks[arrival_day] = {}
                if conn.destination not in self.projected_stocks[arrival_day]:
                    self.projected_stocks[arrival_day][conn.destination] = 0
                self.projected_stocks[arrival_day][conn.destination] += var.varValue

                self.logger.info(
                    f"Scheduled movement: {var.varValue:.1f} units from {conn.source} "
                    f"to {conn.destination}, arriving day {arrival_day}"
                )

        return movements

    def get_solution_stats(self) -> Dict:
        """Get statistics about the current solution"""
        stats = {
            "current_day": self.current_day,
            "is_endgame": self.is_endgame,
            "active_demands": len([d for d in self.demands if d.remaining_amount > 0]),
            "total_demand_volume": sum(
                d.remaining_amount for d in self.demands if d.remaining_amount > 0
            ),
            "projected_deliveries": sum(
                sum(amounts.values())
                for day, amounts in self.projected_stocks.items()
                if day > self.current_day
            ),
            "refinery_utilization": {
                node_id: (node.stock / node.capacity * 100)
                for node_id, node in self.nodes.items()
                if node.type == "refinery"
            },
            "tank_utilization": {
                node_id: (node.stock / node.capacity * 100)
                for node_id, node in self.nodes.items()
                if node.type == "tank"
            },
        }

        # Log detailed stats
        self.logger.info(f"Day {self.current_day} statistics:")
        self.logger.info(f"Active demands: {stats['active_demands']}")
        self.logger.info(f"Total demand volume: {stats['total_demand_volume']:.1f}")
        self.logger.info(f"Projected deliveries: {stats['projected_deliveries']:.1f}")

        for node_id, utilization in stats["refinery_utilization"].items():
            self.logger.info(f"Refinery {node_id}: {utilization:.1f}% full")

        return stats


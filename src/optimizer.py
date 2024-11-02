# src/optimizer.py

import pulp
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpStatus
from typing import Dict, List, Optional
from models import Node, Connection, Demand, Movement
import logging


class Optimizer:
    """Optimizes fuel supply chain movements"""

    def __init__(
        self,
        nodes: Dict[str, Node],
        connections: Dict[str, Connection],
        demands: List[Demand],
        current_day: int,
        planning_horizon: int = 7,
    ):
        self.nodes = nodes
        self.connections = connections
        self.demands = demands
        self.current_day = current_day
        self.planning_horizon = planning_horizon
        self.model: Optional[LpProblem] = None

        # Weights for objective function
        self.cost_weight = 1.0
        self.co2_weight = 0.5

        # Initialize logging
        logging.getLogger(__name__).setLevel(logging.INFO)

    def create_model(self) -> LpProblem:
        """Create the optimization model"""
        model = LpProblem("Fuel_Supply_Chain_Optimization", LpMinimize)

        # Create decision variables for each connection and day in planning horizon
        flow_vars = {}
        for conn_id, conn in self.connections.items():
            for day in range(
                self.current_day, self.current_day + self.planning_horizon
            ):
                var_name = f"flow_{conn_id}_day_{day}"
                flow_vars[(conn_id, day)] = LpVariable(var_name, lowBound=0)

        # Objective function
        total_cost = lpSum(
            [
                flow_vars[(conn_id, day)] * conn.cost_per_unit * conn.distance
                for conn_id, conn in self.connections.items()
                for day in range(
                    self.current_day, self.current_day + self.planning_horizon
                )
                if (conn_id, day) in flow_vars
            ]
        )

        total_co2 = lpSum(
            [
                flow_vars[(conn_id, day)] * conn.co2_per_unit * conn.distance
                for conn_id, conn in self.connections.items()
                for day in range(
                    self.current_day, self.current_day + self.planning_horizon
                )
                if (conn_id, day) in flow_vars
            ]
        )

        model += self.cost_weight * total_cost + self.co2_weight * total_co2

        # Add constraints
        self._add_capacity_constraints(model, flow_vars)
        self._add_demand_constraints(model, flow_vars)
        self._add_node_balance_constraints(model, flow_vars)

        self.model = model
        self.flow_vars = flow_vars
        return model

    def _add_capacity_constraints(self, model: LpProblem, flow_vars: Dict) -> None:
        """Add capacity constraints to the model"""
        # Node storage capacity constraints
        for node_id, node in self.nodes.items():
            for day in range(
                self.current_day, self.current_day + self.planning_horizon
            ):
                inflow = lpSum(
                    [
                        flow_vars.get((conn_id, day - conn.lead_time_days), 0)
                        for conn_id, conn in self.connections.items()
                        if conn.destination == node_id
                        and day - conn.lead_time_days >= self.current_day
                    ]
                )

                outflow = lpSum(
                    [
                        flow_vars.get((conn_id, day), 0)
                        for conn_id, conn in self.connections.items()
                        if conn.source == node_id
                    ]
                )

                # Calculate expected stock level
                stock = node.stock + inflow - outflow

                # Add capacity constraints
                model += stock <= node.capacity, f"Capacity_{node_id}_day_{day}"
                model += stock >= 0, f"NonNegative_{node_id}_day_{day}"

        # Connection capacity constraints
        for conn_id, conn in self.connections.items():
            for day in range(
                self.current_day, self.current_day + self.planning_horizon
            ):
                if (conn_id, day) in flow_vars:
                    model += (
                        flow_vars[(conn_id, day)] <= conn.max_capacity,
                        f"Connection_Capacity_{conn_id}_day_{day}",
                    )

    def _add_demand_constraints(self, model: LpProblem, flow_vars: Dict) -> None:
        """Add demand fulfillment constraints"""
        for demand in self.demands:
            if demand.remaining_amount <= 0:
                continue

            # Only consider demands that fall within our planning horizon
            if (
                demand.start_delivery_day <= self.current_day + self.planning_horizon
                and demand.end_delivery_day >= self.current_day
            ):
                # Calculate total delivery to this customer
                deliveries = []
                for day in range(
                    max(self.current_day, demand.start_delivery_day),
                    min(
                        self.current_day + self.planning_horizon,
                        demand.end_delivery_day + 1,
                    ),
                ):
                    inflow = lpSum(
                        [
                            flow_vars.get((conn_id, day - conn.lead_time_days), 0)
                            for conn_id, conn in self.connections.items()
                            if conn.destination == demand.customer_id
                            and day - conn.lead_time_days >= self.current_day
                        ]
                    )
                    deliveries.append(inflow)

                if (
                    deliveries
                ):  # Only add constraint if deliveries are possible in this period
                    total_delivery = lpSum(deliveries)
                    model += (
                        total_delivery
                        >= min(
                            demand.remaining_amount,
                            self.nodes[demand.customer_id].daily_input,
                        ),
                        f"Demand_{demand.id}_day_{self.current_day}",
                    )

    def _add_node_balance_constraints(self, model: LpProblem, flow_vars: Dict) -> None:
        """Add node flow balance constraints"""
        for node_id, node in self.nodes.items():
            for day in range(
                self.current_day, self.current_day + self.planning_horizon
            ):
                # Total inflow
                inflow = lpSum(
                    [
                        flow_vars.get((conn_id, day - conn.lead_time_days), 0)
                        for conn_id, conn in self.connections.items()
                        if conn.destination == node_id
                        and day - conn.lead_time_days >= self.current_day
                    ]
                )

                # Total outflow
                outflow = lpSum(
                    [
                        flow_vars.get((conn_id, day), 0)
                        for conn_id, conn in self.connections.items()
                        if conn.source == node_id
                    ]
                )

                # Add daily input/output capacity constraints
                if node.daily_input > 0:
                    model += (
                        inflow <= node.daily_input,
                        f"DailyInput_{node_id}_day_{day}",
                    )
                if node.daily_output > 0:
                    model += (
                        outflow <= node.daily_output,
                        f"DailyOutput_{node_id}_day_{day}",
                    )

    def optimize(self) -> List[Dict]:
        """
        Run the optimization and return the movements for the current day

        Returns:
            List[Dict]: List of movements to be executed
        """
        try:
            if not self.model:
                self.create_model()

            # Solve the model
            solver_status = self.model.solve()

            if LpStatus[solver_status] != "Optimal":
                logging.warning(
                    f"Non-optimal solution status: {LpStatus[solver_status]}"
                )
                return []

            # Extract movements for the current day
            movements = []
            for (conn_id, day), var in self.flow_vars.items():
                if day == self.current_day and var.varValue > 0:
                    conn = self.connections[conn_id]
                    movement = {
                        "connectionId": conn_id,
                        "amount": var.varValue,
                        "fromNode": conn.source,
                        "toNode": conn.destination,
                        "postedDay": self.current_day,
                        "leadTime": conn.lead_time_days,
                    }
                    movements.append(movement)

            logging.info(
                f"Generated {len(movements)} movements for day {self.current_day}"
            )
            return movements

        except Exception as e:
            logging.error(f"Error during optimization: {str(e)}")
            return []

    def get_solution_stats(self) -> Dict:
        """Get statistics about the current solution"""
        if not self.model or LpStatus[self.model.status] != "Optimal":
            return {}

        stats = {
            "objective_value": pulp.value(self.model.objective),
            "status": LpStatus[self.model.status],
            "num_variables": len(self.flow_vars),
            "num_constraints": len(self.model.constraints),
            "num_movements": sum(
                1 for var in self.flow_vars.values() if var.varValue > 0
            ),
        }
        return stats


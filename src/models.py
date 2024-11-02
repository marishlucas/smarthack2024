# src/models.py

from dataclasses import dataclass
from typing import Optional


@dataclass
class Node:
    """Represents a node in the supply chain network"""

    id: str
    type: str  # 'refinery', 'tank', or 'customer'
    capacity: float
    daily_output: float
    daily_input: float
    stock: float = 0.0

    def __post_init__(self):
        # Validate node type
        valid_types = {"refinery", "tank", "customer"}
        if self.type.lower() not in valid_types:
            raise ValueError(
                f"Invalid node type: {self.type}. Must be one of {valid_types}"
            )

        # Validate numeric fields
        if self.capacity < 0:
            raise ValueError(f"Capacity cannot be negative: {self.capacity}")
        if self.daily_output < 0:
            raise ValueError(f"Daily output cannot be negative: {self.daily_output}")
        if self.daily_input < 0:
            raise ValueError(f"Daily input cannot be negative: {self.daily_input}")
        if self.stock < 0:
            raise ValueError(f"Stock cannot be negative: {self.stock}")


@dataclass
class Connection:
    """Represents a connection between nodes in the supply chain network"""

    id: str
    source: str
    destination: str
    distance: float
    lead_time_days: int
    connection_type: str
    max_capacity: float
    cost_per_unit: float
    co2_per_unit: float

    def __post_init__(self):
        # Validate numeric fields
        if self.distance <= 0:
            raise ValueError(f"Distance must be positive: {self.distance}")
        if self.lead_time_days <= 0:
            raise ValueError(f"Lead time must be positive: {self.lead_time_days}")
        if self.max_capacity <= 0:
            raise ValueError(f"Max capacity must be positive: {self.max_capacity}")
        if self.cost_per_unit < 0:
            raise ValueError(f"Cost per unit cannot be negative: {self.cost_per_unit}")
        if self.co2_per_unit < 0:
            raise ValueError(f"CO2 per unit cannot be negative: {self.co2_per_unit}")


@dataclass
class Demand:
    """Represents a customer demand"""

    id: str
    customer_id: str
    quantity: float
    post_day: int
    start_delivery_day: int
    end_delivery_day: int
    remaining_amount: Optional[float] = None

    def __post_init__(self):
        # Set remaining amount if not provided
        if self.remaining_amount is None:
            self.remaining_amount = self.quantity

        # Validate numeric fields
        if self.quantity <= 0:
            raise ValueError(f"Quantity must be positive: {self.quantity}")
        if self.post_day < 0:
            raise ValueError(f"Post day cannot be negative: {self.post_day}")
        if self.start_delivery_day < self.post_day:
            raise ValueError("Start delivery day cannot be before post day")
        if self.end_delivery_day < self.start_delivery_day:
            raise ValueError("End delivery day cannot be before start delivery day")
        if self.remaining_amount < 0:
            raise ValueError(
                f"Remaining amount cannot be negative: {self.remaining_amount}"
            )


@dataclass
class Movement:
    """Represents a movement of fuel between nodes"""

    connection_id: str
    amount: float
    posted_day: int
    from_node: str
    to_node: str
    lead_time: int

    def __post_init__(self):
        # Validate numeric fields
        if self.amount <= 0:
            raise ValueError(f"Amount must be positive: {self.amount}")
        if self.posted_day < 0:
            raise ValueError(f"Posted day cannot be negative: {self.posted_day}")
        if self.lead_time <= 0:
            raise ValueError(f"Lead time must be positive: {self.lead_time}")


# Connection type mappings with associated costs and CO2 emissions
CONNECTION_TYPE_MAPPING = {
    "pipeline": {"cost_per_unit": 0.5, "co2_per_unit": 0.2},
    "truck": {"cost_per_unit": 1.0, "co2_per_unit": 0.5},
}


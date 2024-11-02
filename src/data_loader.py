# src/data_loader.py

import pandas as pd
from typing import Set
import logging
from pathlib import Path


class DataLoader:
    """Handles loading and validation of CSV data files"""

    def __init__(self, data_path: str = "data/"):
        self.data_path = data_path

        # Define required columns for each file
        self.required_refinery_columns = {
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

        self.required_tank_columns = {
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

        self.required_customer_columns = {
            "id",
            "name",
            "max_input",
            "over_input_penalty",
            "late_delivery_penalty",
            "early_delivery_penalty",
            "node_type",
        }

        self.required_connection_columns = {
            "id",
            "from_id",
            "to_id",
            "distance",
            "lead_time_days",
            "connection_type",
            "max_capacity",
        }

        self.required_demand_columns = {
            "id",
            "customer_id",
            "quantity",
            "post_day",
            "start_delivery_day",
            "end_delivery_day",
        }

        self.required_team_columns = {"id", "color", "name", "api_key", "internal_use"}

    def load_file(self, filename: str, required_columns: Set[str]) -> pd.DataFrame:
        """
        Load and validate a CSV file

        Args:
            filename: Name of the CSV file to load
            required_columns: Set of required column names

        Returns:
            DataFrame containing the loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If required columns are missing or file is empty
        """
        try:
            file_path = Path(self.data_path) / filename
            df = pd.read_csv(file_path, delimiter=";", encoding="utf-8")

            # Standardize column names
            df.columns = df.columns.str.strip().str.lower()

            # Validate columns
            missing_cols = required_columns - set(df.columns)
            if missing_cols:
                raise ValueError(
                    f"Missing required columns in {filename}: {missing_cols}"
                )

            # Check if file is empty
            if df.empty:
                raise ValueError(f"{filename} is empty")

            logging.info(f"Successfully loaded {filename}")
            return df

        except FileNotFoundError:
            logging.error(f"Could not find {filename} in {self.data_path}")
            raise FileNotFoundError(f"Could not find {filename} in {self.data_path}")
        except pd.errors.EmptyDataError:
            logging.error(f"{filename} is empty")
            raise ValueError(f"{filename} is empty")
        except Exception as e:
            logging.error(f"Error loading {filename}: {str(e)}")
            raise

    def load_refineries(self) -> pd.DataFrame:
        """Load refineries data"""
        return self.load_file("refineries.csv", self.required_refinery_columns)

    def load_tanks(self) -> pd.DataFrame:
        """Load storage tanks data"""
        return self.load_file("tanks.csv", self.required_tank_columns)

    def load_customers(self) -> pd.DataFrame:
        """Load customers data"""
        return self.load_file("customers.csv", self.required_customer_columns)

    def load_connections(self) -> pd.DataFrame:
        """Load connections data"""
        return self.load_file("connections.csv", self.required_connection_columns)

    def load_demands(self) -> pd.DataFrame:
        """Load demands data"""
        return self.load_file("demands.csv", self.required_demand_columns)

    def load_teams(self) -> pd.DataFrame:
        """Load teams data"""
        return self.load_file("teams.csv", self.required_team_columns)

    def validate_data_types(self, df: pd.DataFrame, filename: str) -> None:
        """
        Validate data types for numeric columns

        Args:
            df: DataFrame to validate
            filename: Name of the file for error reporting

        Raises:
            ValueError: If numeric data validation fails
        """
        numeric_columns = df.select_dtypes(include=["float64", "int64"]).columns
        for col in numeric_columns:
            if df[col].isnull().any():
                raise ValueError(f"Column {col} in {filename} contains null values")
            if (df[col] < 0).any():
                raise ValueError(f"Column {col} in {filename} contains negative values")


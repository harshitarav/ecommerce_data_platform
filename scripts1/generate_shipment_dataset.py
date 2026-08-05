import os
import pandas as pd
from collections import defaultdict

# ==========================================================
# PROJECT FOLDERS
# ==========================================================

DATASET_FOLDER = os.path.join("..", "source_data")
OUTPUT_FOLDER = os.path.join("..", "generated_data")


# ==========================================================
# LOAD DATASETS
# ==========================================================

def load_datasets():

    datasets = {

        "customers": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_customers_dataset.csv")
        ),

        "geolocation": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_geolocation_dataset.csv")
        ),

        "order_items": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_order_items_dataset.csv")
        ),

        "payments": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_order_payments_dataset.csv")
        ),

        "reviews": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_order_reviews_dataset.csv")
        ),

        "orders": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_orders_dataset.csv")
        ),

        "products": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_products_dataset.csv")
        ),

        "sellers": pd.read_csv(
            os.path.join(DATASET_FOLDER, "olist_sellers_dataset.csv")
        ),

        "category_translation": pd.read_csv(
            os.path.join(
                DATASET_FOLDER,
                "product_category_name_translation.csv"
            )
        ),

        "inventory": pd.read_csv(
            os.path.join(
                DATASET_FOLDER,
                "inventory_initial_production_v2.csv"
            )
        )

    }

    print("\n==============================")
    print("DATASETS LOADED")
    print("==============================")

    for name, df in datasets.items():

        print(
            f"{name:<22}"
            f"Rows = {len(df):>8}"
            f"   Columns = {len(df.columns)}"
        )

    return datasets


# ==========================================================
# VALIDATE DATASETS
# ==========================================================

def validate_datasets(datasets):

    print("\n==============================")
    print("VALIDATING DATASETS")
    print("==============================")

    for name, df in datasets.items():

        if df.empty:
            raise Exception(f"{name} dataset is empty.")

        print(f"{name:<22} PASSED")

    print("\nAll datasets validated successfully.")

# ==========================================================
# CREATE PRODUCT -> WAREHOUSE MAPPING
# ==========================================================

def create_warehouse_mapping(inventory_df):

    warehouse_mapping = defaultdict(list)

    inventory_df = inventory_df.sort_values(
        ["product_id", "warehouse_id"]
    )

    for _, row in inventory_df.iterrows():

        warehouse_mapping[row["product_id"]].append({

            "warehouse_id": row["warehouse_id"],
            "warehouse_city": row["warehouse_city"],
            "warehouse_state": row["warehouse_state"]

        })

    print("\n==============================")
    print("WAREHOUSE MAPPING CREATED")
    print("==============================")

    print(
        f"Unique Products : {len(warehouse_mapping)}"
    )

    return warehouse_mapping


# ==========================================================
# ASSIGN WAREHOUSE USING ROUND ROBIN
# ==========================================================

def assign_warehouse_to_order_items(
    order_items_df,
    warehouse_mapping
):

    print("\n==============================")
    print("ASSIGNING WAREHOUSES")
    print("==============================")

    product_counter = defaultdict(int)

    assigned_rows = []

    for _, row in order_items_df.iterrows():

        product_id = row["product_id"]

        warehouses = warehouse_mapping.get(product_id)

        if not warehouses:

            continue

        index = (
            product_counter[product_id]
            %
            len(warehouses)
        )

        selected = warehouses[index]

        product_counter[product_id] += 1

        assigned_rows.append({

            "order_id": row["order_id"],

            "order_item_id": row["order_item_id"],

            "product_id": row["product_id"],

            "seller_id": row["seller_id"],

            "shipping_cost": row["freight_value"],

            "warehouse_id": selected["warehouse_id"],

            "warehouse_city": selected["warehouse_city"],

            "warehouse_state": selected["warehouse_state"]

        })

    assigned_df = pd.DataFrame(
        assigned_rows
    )

    print(
        f"Assigned Rows : {len(assigned_df)}"
    )

    return assigned_df

# ==========================================================
# GENERATE SHIPMENT IDS
# ==========================================================

def generate_shipment_ids(assigned_df):

    print("\n==============================")
    print("GENERATING SHIPMENT IDS")
    print("==============================")

    assigned_df = assigned_df.copy()

    assigned_df["shipment_group"] = (
        assigned_df["order_id"].astype(str)
        + "_"
        + assigned_df["warehouse_id"].astype(str)
    )

    shipment_lookup = {}

    shipment_counter = 1

    shipment_ids = []

    for group in assigned_df["shipment_group"]:

        if group not in shipment_lookup:

            shipment_lookup[group] = (
                "SHIP"
                + str(shipment_counter).zfill(8)
            )

            shipment_counter += 1

        shipment_ids.append(
            shipment_lookup[group]
        )

    assigned_df["shipment_id"] = shipment_ids

    assigned_df.drop(
        columns=["shipment_group"],
        inplace=True
    )

    print(
        f"Unique Shipments : "
        f"{assigned_df['shipment_id'].nunique()}"
    )

    return assigned_df

# ==========================================================
# GENERATE TRACKING NUMBERS
# ==========================================================

def generate_tracking_numbers(shipment_df):

    print("\n==============================")
    print("GENERATING TRACKING NUMBERS")
    print("==============================")

    tracking_lookup = {}

    tracking_counter = 1

    tracking_numbers = []

    for shipment in shipment_df["shipment_id"]:

        if shipment not in tracking_lookup:

            tracking_lookup[shipment] = (
                "TRK2026"
                + str(tracking_counter).zfill(10)
            )

            tracking_counter += 1

        tracking_numbers.append(
            tracking_lookup[shipment]
        )

    shipment_df["tracking_number"] = tracking_numbers

    print(
        f"Tracking Numbers Generated : "
        f"{shipment_df['tracking_number'].nunique()}"
    )

    return shipment_df

# ==========================================================
# ASSIGN CARRIERS
# ==========================================================

def assign_carrier(shipment_df):

    print("\n==============================")
    print("ASSIGNING CARRIERS")
    print("==============================")

    carrier_list = [

        "DHL",
        "FedEx",
        "UPS",
        "Blue Dart",
        "Delhivery",
        "DTDC",
        "XpressBees",
        "Ekart"

    ]

    warehouse_ids = sorted(
        shipment_df["warehouse_id"].unique()
    )

    carrier_map = {}

    for i, warehouse in enumerate(warehouse_ids):

        carrier_map[warehouse] = carrier_list[
            i % len(carrier_list)
        ]

    shipment_df["carrier_name"] = (
        shipment_df["warehouse_id"]
        .map(carrier_map)
    )

    print("Carrier Assignment Completed.")

    return shipment_df

# ==========================================================
# ENRICH SHIPMENT DATASET
# ==========================================================

def enrich_shipments(shipment_df, orders_df):

    print("\n==============================")
    print("ENRICHING SHIPMENTS")
    print("==============================")

    orders_subset = orders_df[[

        "order_id",

        "order_status",

        "order_delivered_carrier_date",

        "order_delivered_customer_date"

    ]]

    shipment_df = shipment_df.merge(

        orders_subset,

        on="order_id",

        how="left"

    )

    shipment_df.rename(

        columns={

            "order_status": "shipment_status",

            "order_delivered_carrier_date": "shipped_timestamp",

            "order_delivered_customer_date": "delivered_timestamp"

        },

        inplace=True

    )

    status_mapping = {

        "created": "Processing",

        "approved": "Packed",

        "invoiced": "Packed",

        "processing": "Packed",

        "shipped": "Shipped",

        "delivered": "Delivered",

        "canceled": "Cancelled",

        "unavailable": "Cancelled"

    }

    shipment_df["shipment_status"] = (

        shipment_df["shipment_status"]

        .map(status_mapping)

        .fillna("Shipped")

    )

    shipment_df = shipment_df[[

        "shipment_id",

        "tracking_number",

        "order_id",

        "order_item_id",

        "product_id",

        "seller_id",

        "warehouse_id",

        "warehouse_city",

        "warehouse_state",

        "carrier_name",

        "shipping_cost",

        "shipment_status",

        "shipped_timestamp",

        "delivered_timestamp"

    ]]

    print("Shipment dataset enriched.")

    return shipment_df

# ==========================================================
# EXPORT SHIPMENT DATASET
# ==========================================================

def export_shipment_dataset(shipment_df):

    print("\n==============================")
    print("EXPORTING SHIPMENT DATASET")
    print("==============================")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    output_path = os.path.join(
        OUTPUT_FOLDER,
        "shipment_management_system.csv"
    )

    shipment_df.to_csv(
        output_path,
        index=False
    )

    print(f"Rows Exported : {len(shipment_df)}")
    print(f"Columns Exported : {len(shipment_df.columns)}")
    print(f"\nSaved successfully to:\n{output_path}")

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    datasets = load_datasets()

    validate_datasets(datasets)

    warehouse_mapping = create_warehouse_mapping(
        datasets["inventory"]
    )

    assigned_df = assign_warehouse_to_order_items(
        datasets["order_items"],
        warehouse_mapping
    )

    shipment_df = generate_shipment_ids(
        assigned_df
    )

    shipment_df = generate_tracking_numbers(
        shipment_df
    )

    shipment_df = assign_carrier(
        shipment_df
    )

    print("\nShipment Preview\n")

    print(
        shipment_df.head(10)
    )
    shipment_df = enrich_shipments(
    shipment_df,
    datasets["orders"]
    )

    print("\nFinal Shipment Dataset\n")

    print(shipment_df.head(10))

    export_shipment_dataset(
    shipment_df
    )

    











# //////////////////////////////////////////////////////////////////////////////////
# import os
# import pandas as pd

# # ==============================
# # Project Folders
# # ==============================

# DATASET_FOLDER = os.path.join("..", "datasets")
# OUTPUT_FOLDER = os.path.join("..", "generated_data")


# # ==============================
# # Load all datasets
# # ==============================

# def load_datasets():

#     datasets = {

#         "customers": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_customers_dataset.csv")
#         ),

#         "geolocation": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_geolocation_dataset.csv")
#         ),

#         "order_items": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_order_items_dataset.csv")
#         ),

#         "payments": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_order_payments_dataset.csv")
#         ),

#         "reviews": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_order_reviews_dataset.csv")
#         ),

#         "orders": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_orders_dataset.csv")
#         ),

#         "products": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_products_dataset.csv")
#         ),

#         "sellers": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "olist_sellers_dataset.csv")
#         ),

#         "category_translation": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "product_category_name_translation.csv")
#         ),

#         "inventory": pd.read_csv(
#             os.path.join(DATASET_FOLDER, "inventory_initial_production_v2.csv")
#         )

#     }

#     print("\nAll datasets loaded successfully.\n")

#     for name, df in datasets.items():
#         print(f"{name:<22} Rows = {len(df):>8}   Columns = {len(df.columns)}")

#     return datasets


# # ==============================
# # Create Product -> Warehouse Mapping
# # ==============================

# def create_warehouse_mapping(inventory_df):

#     warehouse_mapping = {}

#     for _, row in inventory_df.iterrows():

#         product_id = row["product_id"]

#         warehouse_info = {
#             "warehouse_id": row["warehouse_id"],
#             "warehouse_city": row["warehouse_city"],
#             "warehouse_state": row["warehouse_state"],
#             "available_stock": row["available_stock"]
#         }

#         if product_id not in warehouse_mapping:
#             warehouse_mapping[product_id] = []

#         warehouse_mapping[product_id].append(warehouse_info)

#     print("\nWarehouse mapping created successfully.")
#     print(f"Total Products in Mapping : {len(warehouse_mapping)}")

#     return warehouse_mapping


# # ==============================
# # Main
# # ==============================


# # ==============================
# # Assign Warehouse to Order Items
# # ==============================

# def assign_warehouse_to_order_items(order_items_df, warehouse_mapping):

#     assigned_shipments = []

#     for _, row in order_items_df.iterrows():

#         product_id = row["product_id"]

#         # Skip if product doesn't exist in inventory
#         if product_id not in warehouse_mapping:
#             continue

#         warehouses = warehouse_mapping[product_id]

#         # Choose warehouse with highest available quantity
#         selected_warehouse = max(
#             warehouses,
#             key=lambda x: x["available_stock"]
#         )

#         assigned_shipments.append({

#             "order_id": row["order_id"],
#             "order_item_id": row["order_item_id"],
#             "product_id": product_id,
#             "seller_id": row["seller_id"],
#             "freight_value": row["freight_value"],

#             "warehouse_id": selected_warehouse["warehouse_id"],
#             "warehouse_city": selected_warehouse["warehouse_city"],
#             "warehouse_state": selected_warehouse["warehouse_state"]

#         })

#     assigned_df = pd.DataFrame(assigned_shipments)

#     print("\nWarehouse assigned successfully.")
#     print(f"Total Order Items Assigned : {len(assigned_df)}")

#     return assigned_df

# # ==============================
# # Generate Shipment IDs
# # ==============================

# def generate_shipment_ids(assigned_df):

#     assigned_df = assigned_df.copy()

#     assigned_df["shipment_group"] = (
#         assigned_df["order_id"] + "_" + assigned_df["warehouse_id"]
#     )

#     shipment_lookup = {
#         group: f"SHIP{str(i+1).zfill(8)}"
#         for i, group in enumerate(
#             assigned_df["shipment_group"].unique()
#         )
#     }

#     assigned_df["shipment_id"] = (
#         assigned_df["shipment_group"]
#         .map(shipment_lookup)
#     )

#     assigned_df.drop(columns=["shipment_group"], inplace=True)

#     print("\nShipment IDs generated successfully.")
#     print(
#         f"Total Shipments : "
#         f"{assigned_df['shipment_id'].nunique()}"
#     )

#     return assigned_df

# # ==============================
# # Enrich Shipment Dataset
# # ==============================

# def enrich_shipment_dataset(shipment_df, orders_df):

#     shipment_df = shipment_df.merge(

#         orders_df[
#             [
#                 "order_id",
#                 "order_status",
#                 "order_delivered_carrier_date",
#                 "order_delivered_customer_date"
#             ]
#         ],

#         on="order_id",
#         how="left"

#     )

#     shipment_df.rename(
#         columns={
#             "order_status": "shipment_status",
#             "order_delivered_carrier_date": "shipped_timestamp",
#             "order_delivered_customer_date": "delivered_timestamp"
#         },
#         inplace=True
#     )

#     print("\nShipment dataset enriched successfully.")

#     return shipment_df

# # ==============================
# # Main
# # ==============================

# if __name__ == "__main__":

#     datasets = load_datasets()
#     print(datasets["inventory"].columns.tolist())

#     warehouse_mapping = create_warehouse_mapping(
#         datasets["inventory"]
#     )

#     assigned_order_items = assign_warehouse_to_order_items(
#         datasets["order_items"],
#         warehouse_mapping
#     )

#     shipment_df = generate_shipment_ids(
#     assigned_order_items
#     )

#     shipment_df = enrich_shipment_dataset(
#     shipment_df,
#     datasets["orders"]
#     )

#     print("\nShipment Dataset Preview:\n")
#     print(shipment_df.head(10))
#     print(shipment_df.columns.tolist())




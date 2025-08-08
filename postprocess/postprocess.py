import argparse
import os
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import List, Dict
from dash import Dash, dcc, html, dash_table
import plotly.graph_objs as go

DDB_OPERATIONS={"Query", "BatchWriteItem", "DeleteItem", "GetItem", "PutItem", "Scan"}
REGIONS = set()
TABLES = set()

def __print_tuple_keyed_dict(d):
    printable = {str(k): v for k, v in d.items()}
    print(json.dumps(printable, indent=2))

def parse_table_metadata(log_file_path: str):
    if not os.path.exists(log_file_path):
        print(f"[ERROR] Log file not found: {log_file_path}")
        return

    with open(log_file_path, "r") as f:
        lines = f.readlines()
        
    current_region = None
    current_table = None
    json_buffer = []
    inside_table = False

    for line in lines:
        line = line.strip()

        # === Table: Name (Region: us-east-1) ===
        if line.startswith("=== Table:"):
            # flush and process previous block if any
            if current_region and current_table and json_buffer:
                process_json_block(current_region, current_table, json_buffer)
            json_buffer = []
            inside_table = True

            match = re.match(r"=== Table: (.*?) \(Region: (.*?)\) ===", line)
            if match:
                current_table = match.group(1)
                current_region = match.group(2)

        elif inside_table and "|" in line:
            # Remove the leading "Region: us-east-1 | "
            _, json_part = line.split("|", 1)
            json_buffer.append(json_part.strip())

    # Final flush
    if current_region and current_table and json_buffer:
        process_json_block(current_region, current_table, json_buffer)

    return SampleMetaData

def process_json_block(region, table_name, lines):
    try:
        full_json = json.loads("\n".join(lines))
        table_data = full_json.get("Table", {})
        update_sample_metadata(region, table_name, table_data)
    except json.JSONDecodeError as e:
        print(f"[JSON ERROR] {region}/{table_name}: {e}")
    except Exception as e:
        print(f"[UNEXPECTED ERROR] {region}/{table_name}: {e}")

def update_sample_metadata(region, table_name, table_data):
    # Init table if not already present
    if table_name not in SampleMetaData['Tables']:
        SampleMetaData['Tables'][table_name] = {}

    existing = SampleMetaData['Tables'][table_name].get(region, {})

    key_schema = table_data.get("KeySchema", [])
    throughput = table_data.get("ProvisionedThroughput", {})
    class_summary = table_data.get("TableClassSummary") or {}
    stream_spec = table_data.get("StreamSpecification") or {}

    def max_or_existing(field, new_value):
        old_value = existing.get(field)
        if old_value is None:
            return new_value
        if new_value is None:
            return old_value
        return max(old_value, new_value)

    def prefer_truthy(field, new_value):
        return new_value if not existing.get(field) else existing[field]

    SampleMetaData['Tables'][table_name][region] = {
        "ItemCount": max_or_existing("ItemCount", table_data.get("ItemCount")),
        "KeySchema": prefer_truthy("KeySchema", key_schema),
        "RCU": max_or_existing("RCU", throughput.get("ReadCapacityUnits")),
        "WCU": max_or_existing("WCU", throughput.get("WriteCapacityUnits")),
        "NumDecreases": max_or_existing("NumDecreases", throughput.get("NumberOfDecreasesToday")),
        "HasLocalSecondaryIndexes": existing.get("HasLocalSecondaryIndexes", False) or bool(table_data.get("LocalSecondaryIndexes")),
        "HasReplicas": existing.get("HasReplicas", False) or bool(table_data.get("Replicas")),
        "StreamsEnabled": existing.get("StreamsEnabled", False) or stream_spec.get("StreamEnabled", False),
        "TableClassSummary": prefer_truthy("TableClassSummary", class_summary.get("TableClass", ""))
    }

def traverse(request_table=None):
    global SAMPLES
    SAMPLES = [
        name for name in os.listdir(BASE_DIR)
        if name.startswith("dynamo_metrics_logs_") and os.path.isdir(os.path.join(BASE_DIR, name))
    ]

    for sample in SAMPLES:
        sample_path = os.path.join(BASE_DIR, sample)

        for region in os.listdir(sample_path):
            region_path = os.path.join(sample_path, region)
            if not os.path.isdir(region_path):
                continue
            REGIONS.add(region)

            for table in os.listdir(region_path):
                table_path = os.path.join(region_path, table)
                if os.path.isdir(table_path):
                    if request_table:
                        if table == request_table:
                            TABLES.add(table)
                    else:
                        TABLES.add(table)

def process_data():
    global SampleCountData
    global SampleP99Data
    global SampleMetaData

    SampleCountData = {op: [] for op in DDB_OPERATIONS} 
    SampleP99Data = {op: [] for op in DDB_OPERATIONS}  
    SampleMetaData = {op: [] for op in DDB_OPERATIONS}  
    SampleMetaData['Tables'] = {}

    for operation in DDB_OPERATIONS:
        for table in TABLES:
            for sample in SAMPLES:
                for region in REGIONS:
                    parse_table_metadata(os.path.join(BASE_DIR, sample, "table_detailed.log"))
                    sample_count_path = os.path.join(BASE_DIR, sample, region, table, operation, "sample_count")
                    if not os.path.isdir(sample_count_path):
                        continue

                    for record in os.listdir(sample_count_path):
                        sample_record = os.path.join(sample_count_path, record)
                        try:
                            with open(sample_record, 'r') as f:
                                data = json.load(f)
                                for datapoint in data.get("Datapoints", []):
                                    SampleCountData[operation].append({
                                        "timestamp": datapoint["Timestamp"],
                                        "value": datapoint.get("SampleCount"),
                                        "table": table
                                    })
                        except Exception as e:
                            print(f"Error reading {sample_record}: {e}")
    
    for operation in DDB_OPERATIONS:
        for table in TABLES:
            for sample in SAMPLES:
                for region in REGIONS:
                    p99_path = os.path.join(BASE_DIR, sample, region, table, operation, "p99_latency")

                    if not os.path.isdir(p99_path):
                        continue

                    for record in os.listdir(p99_path):
                        sample_record = os.path.join(p99_path, record)
                        try:
                            with open(sample_record, 'r') as f:
                                data = json.load(f)
                                for datapoint in data.get("Datapoints", []):
                                    SampleP99Data[operation].append({
                                        "timestamp": datapoint["Timestamp"],
                                        "value": datapoint.get("ExtendedStatistics", {}).get("p99"),
                                        "table": table
                                    })
                        except Exception as e:
                            print(f"Error reading {sample_record}: {e}")
    
    # Save both outputs to disk
    with open("./data/sample_count_data.json", "w") as f:
        json.dump(SampleCountData, f, indent=2)

    with open("./data/sample_p99_data.json", "w") as f:
        json.dump(SampleP99Data, f, indent=2)

    with open("./data/sample_metadata.json", "w") as f:
        json.dump(SampleMetaData, f, indent=2)

def run_dash_ui():
    # Load JSON data
    with open("./data/sample_count_data.json") as f:
        sample_count_data = json.load(f)

    with open("./data/sample_p99_data.json") as f:
        sample_p99_data = json.load(f)

    with open("./data/sample_metadata.json") as f:
        sample_metadata = json.load(f)

    # Extract metadata
    TABLES = sorted({dp["table"] for ops in sample_count_data.values() for dp in ops})
    REGIONS = sorted({dp.get("region", "unknown") for ops in sample_count_data.values() for dp in ops})
    SAMPLES = sorted({record for op in sample_count_data.values() for record in op if "sample" in record})
    DDB_OPERATIONS = list(sample_count_data.keys())

    # Helper to build traces per operation, grouped by table
    def build_traces(records_by_op):
        traces_by_op = {}
        for operation, records in records_by_op.items():
            grouped_by_table = {}
            for dp in records:
                grouped_by_table.setdefault(dp["table"], []).append(dp)

            traces = []
            for table, dps in grouped_by_table.items():
                dps.sort(key=lambda x: x["timestamp"])
                traces.append(go.Scatter(
                    x=[p["timestamp"] for p in dps],
                    y=[p["value"] for p in dps],
                    mode='lines+markers',
                    name=table,
                    legendgroup=table,
                    showlegend=True
                ))
            traces_by_op[operation] = traces
        return traces_by_op

    # Generate all graph components per operation
    def build_graphs(title_prefix, traces_by_op):
        graphs = []
        for operation in DDB_OPERATIONS:
            figure = {
                "data": traces_by_op.get(operation, []),
                "layout": go.Layout(
                    title=f"{title_prefix} - {operation}",
                    xaxis={"title": "Timestamp"},
                    yaxis={"title": "Value"},
                    legend={"itemsizing": "constant"}
                )
            }
            graphs.append(dcc.Graph(id=f"{title_prefix.lower()}-{operation.lower()}", figure=figure))
        return graphs

    def format_key_schema(schema_list):
        parts = []
        for entry in schema_list:
            if entry["KeyType"] == "HASH":
                parts.append(f"HASH: {entry['AttributeName']}")
            elif entry["KeyType"] == "RANGE":
                parts.append(f"RANGE: {entry['AttributeName']}")
        return ", ".join(parts)

    def generate_metadata_tab(tables_metadata):
        cards = []
        for table_name, region_data in tables_metadata.items():
            region_sections = []
            for region, metadata in region_data.items():
                # Create flat dict for display
                display_data = {
                    "Region": region,
                    "ItemCount": metadata.get("ItemCount"),
                    "RCU": metadata.get("RCU"),
                    "WCU": metadata.get("WCU"),
                    "NumDecreases": metadata.get("NumDecreases"),
                    "HasLSI": metadata.get("HasLocalSecondaryIndexes"),
                    "HasReplicas": metadata.get("HasReplicas"),
                    "StreamsEnabled": metadata.get("StreamsEnabled"),
                    "TableClass": metadata.get("TableClassSummary"),
                    "KeySchema": format_key_schema(metadata.get("KeySchema", [])),
                }

                region_sections.append(dash_table.DataTable(
                    columns=[{"name": k, "id": k} for k in display_data],
                    data=[display_data],
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "left", "padding": "5px"},
                    style_header={"fontWeight": "bold"},
                ))

            cards.append(html.Div([
                html.H4(table_name),
                html.Div(region_sections, style={"marginBottom": "40px"})
            ]))

        return html.Div(cards, style={"padding": "20px"})

    tables_metadata = sample_metadata.get("Tables", {})


    # Build traces
    count_traces = build_traces(sample_count_data)
    p99_traces = build_traces(sample_p99_data)

    # Build the app
    app = Dash(__name__)
    app.layout = html.Div([
        html.H1("DynamoDB Metrics Viewer"),

        dcc.Tabs([
            dcc.Tab(label="Sample Count", children=build_graphs("Sample Count", count_traces)),
            dcc.Tab(label="P99 Latency", children=build_graphs("P99 Latency", p99_traces)),
            dcc.Tab(label="Metadata", children=generate_metadata_tab(tables_metadata))
        ])
    ])

    app.run(debug=True)

def main(operation, folder=None, table=None):
    if operation=="load":
        global BASE_DIR
        BASE_DIR = folder
        traverse(table)
        process_data()
    elif operation=="UI":
        run_dash_ui()

def parse_args():
    parser = argparse.ArgumentParser(description="CSV Folder Processor and Dash Visualizer (no pandas)")
    parser.add_argument('operation', type=str, help="Load the UI or Process the Data", choices=["load", "UI"])
    parser.add_argument('--folder', type=str, help="Root folder with CSV files", default=None)
    parser.add_argument('--table', type=str, help="Optional field for limiting the output")
    return parser.parse_args() 

if __name__ == "__main__":
    args = parse_args()
    main(args.operation, args.folder, args.table)

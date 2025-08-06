import argparse
import os
import csv
import json
from pathlib import Path
from typing import List, Dict
from dash import Dash, dcc, html
import plotly.graph_objs as go

DDB_OPERATIONS={"Query", "BatchWriteItem", "DeleteItem", "GetItem", "PutItem", "Scan"}
REGIONS = set()
TABLES = set()

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

    SampleCountData = {op: [] for op in DDB_OPERATIONS} 
    SampleP99Data = {op: [] for op in DDB_OPERATIONS}  

    for operation in DDB_OPERATIONS:
        for table in TABLES:
            for sample in SAMPLES:
                for region in REGIONS:
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

def run_dash_ui():
    # Load JSON data
    with open("./data/sample_count_data.json") as f:
        sample_count_data = json.load(f)

    with open("./data/sample_p99_data.json") as f:
        sample_p99_data = json.load(f)

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
            dcc.Tab(label="Metadata", children=[
                html.H4("Metadata Summary"),
                html.Pre(f"Tables:\n{json.dumps(TABLES, indent=2)}\n\nRegions:\n{json.dumps(REGIONS, indent=2)}")
            ])
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

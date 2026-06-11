#!/usr/bin/env python3
import argparse
import os
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

from dash import Dash, dcc, html, dash_table
import plotly.graph_objs as go

# ----------------- Globals -----------------
DDB_OPERATIONS = {
    "Query", "BatchWriteItem", "DeleteItem", "GetItem",
    "PutItem", "Scan", "UpdateItem",
}
REGIONS = set()
TABLES = set()
SAMPLES = []
BASE_DIR = ""

def __print_tuple_keyed_dict(d):
    printable = {str(k): v for k, v in d.items()}
    print(json.dumps(printable, indent=2))


def parse_metric_timestamp(timestamp_str: str):
    """Parse CloudWatch CSV timestamps, including offsets like +05:30."""
    if not timestamp_str:
        return None
    try:
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    return None


def detect_file_format(file_path: str) -> str:
    """Return 'json', 'csv', or 'unknown' for a metric log file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return "unknown"
        try:
            data = json.loads(content)
            if isinstance(data, dict) and "Datapoints" in data:
                return "json"
        except (json.JSONDecodeError, ValueError):
            pass
        if "DATAPOINTS" in content or "EXTENDEDSTATISTICS" in content:
            return "csv"
        # CloudWatch header with no datapoints (no traffic in window)
        if content.startswith("SuccessfulRequestLatency"):
            return "csv"
    except OSError as e:
        print(f"Error detecting format for {file_path}: {e}")
    return "unknown"


def parse_csv_log_file(file_path: str) -> List[Tuple[datetime, float]]:
    """Parse tab-delimited CloudWatch CSV metric output."""
    timestamps_values: List[Tuple[datetime, float]] = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.read().strip().split("\n")
    except OSError as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return timestamps_values

    is_sample_count = "sample_count" in file_path or "SampleCount" in file_path
    is_p99_latency = "p99_latency" in file_path or "P99" in file_path
    current_timestamp = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("DATAPOINTS"):
            parts = line.split("\t")
            if len(parts) < 2:
                parts = line.split()

            if is_sample_count and len(parts) >= 3:
                try:
                    value = float(parts[1])
                    timestamp = parse_metric_timestamp(parts[2])
                    if timestamp is not None:
                        timestamps_values.append((timestamp, value))
                except (ValueError, TypeError, IndexError):
                    pass
            elif is_p99_latency and len(parts) >= 2:
                timestamp = parse_metric_timestamp(parts[1])
                if timestamp is not None:
                    current_timestamp = timestamp

        elif line.startswith("EXTENDEDSTATISTICS") and current_timestamp is not None:
            parts = line.split("\t")
            if len(parts) < 2:
                parts = line.split()
            if len(parts) >= 2:
                try:
                    value = float(parts[1])
                    timestamps_values.append((current_timestamp, value))
                    current_timestamp = None
                except (ValueError, TypeError):
                    pass

    return timestamps_values


def load_metric_datapoints(file_path: str, metric: str) -> List[Tuple[str, float]]:
    """
    Load (timestamp, value) pairs from a JSON or CSV metric log file.
    metric: 'Count' for sample_count, 'P99' for p99_latency.
    """
    file_format = detect_file_format(file_path)
    datapoints: List[Tuple[str, float]] = []

    if file_format == "json":
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for dp in data.get("Datapoints", []):
                ts = dp.get("Timestamp")
                if metric == "Count":
                    val = dp.get("SampleCount")
                else:
                    val = (dp.get("ExtendedStatistics") or {}).get("p99")
                if ts is not None and val is not None:
                    datapoints.append((ts, float(val)))
        except Exception as e:
            print(f"Error reading JSON file {file_path}: {e}")
    elif file_format == "csv":
        for ts, val in parse_csv_log_file(file_path):
            datapoints.append((ts.isoformat(), val))
    else:
        print(f"Warning: Unknown format for {file_path}")

    return datapoints


def _ascii_field_value(line: str, field: str):
    match = re.search(rf"{re.escape(field)}\s*\|\s*([^|]+?)\s*\|", line)
    return match.group(1).strip() if match else None


def parse_ascii_table_metadata(content: str):
    """Parse ASCII table_detailed.log output from Bash/Go collectors."""
    current_region = None
    current_table = None
    in_key_schema = False
    key_schema: List[dict] = []
    table_data: dict = {}

    def flush_table():
        nonlocal table_data, key_schema
        if current_region and current_table and table_data:
            table_data["KeySchema"] = key_schema
            update_sample_metadata(current_region, current_table, table_data)
        table_data = {}
        key_schema = []

    for raw in content.split("\n"):
        line = raw.strip()
        if not line:
            continue

        if line.startswith("=== Table:"):
            flush_table()
            in_key_schema = False
            match = re.match(r"=== Table: (.*?) \(Region: (.*?)\) ===", line)
            if match:
                current_table = match.group(1)
                current_region = match.group(2)
            continue

        if current_table is None:
            continue

        if "KeySchema" in line and "AttributeName" not in line:
            in_key_schema = True
            continue
        if in_key_schema:
            key_match = re.search(r"\|\|\s+(\S+)\s+\|\s+(HASH|RANGE)", line)
            if key_match:
                key_schema.append({
                    "AttributeName": key_match.group(1),
                    "KeyType": key_match.group(2),
                })
                continue
            if line.startswith("Region:") and "ProvisionedThroughput" in line:
                in_key_schema = False

        if re.search(r"\bItemCount\b", line):
            val = _ascii_field_value(line, "ItemCount")
            if val and val.isdigit():
                table_data["ItemCount"] = int(val)
        elif "ReadCapacityUnits" in line:
            val = _ascii_field_value(line, "ReadCapacityUnits")
            if val and val.isdigit():
                table_data.setdefault("ProvisionedThroughput", {})["ReadCapacityUnits"] = int(val)
        elif "WriteCapacityUnits" in line:
            val = _ascii_field_value(line, "WriteCapacityUnits")
            if val and val.isdigit():
                table_data.setdefault("ProvisionedThroughput", {})["WriteCapacityUnits"] = int(val)
        elif "NumberOfDecreasesToday" in line:
            val = _ascii_field_value(line, "NumberOfDecreasesToday")
            if val and val.isdigit():
                table_data.setdefault("ProvisionedThroughput", {})["NumberOfDecreasesToday"] = int(val)
        elif "StreamEnabled" in line:
            val = _ascii_field_value(line, "StreamEnabled")
            if val is not None:
                table_data["StreamSpecification"] = {"StreamEnabled": val.lower() == "true"}
        elif "BillingMode" in line and "Summary" not in line:
            val = _ascii_field_value(line, "BillingMode")
            if val:
                table_data["BillingModeSummary"] = {"BillingMode": val}
        elif "LocalSecondaryIndexes" in line and "None" not in line:
            table_data["LocalSecondaryIndexes"] = [{"IndexName": "LSI"}]

    flush_table()


def parse_json_table_metadata(log_file_path: str):
    """Parse JSON-embedded table_detailed.log output."""
    current_region = None
    current_table = None
    json_buffer: List[str] = []
    inside_table = False

    def flush():
        nonlocal current_region, current_table, json_buffer
        if current_region and current_table and json_buffer:
            process_json_block(current_region, current_table, json_buffer)
        json_buffer = []

    with open(log_file_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()

            if line.startswith("=== Table:"):
                flush()
                inside_table = True
                match = re.match(r"=== Table: (.*?) \(Region: (.*?)\) ===", line)
                if match:
                    current_table = match.group(1)
                    current_region = match.group(2)
                continue

            if inside_table and "|" in line:
                _, json_part = line.split("|", 1)
                json_buffer.append(json_part.strip())

    flush()


def parse_table_metadata(log_file_path: str):
    if not os.path.exists(log_file_path):
        return

    with open(log_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    if "=== Table:" in content and "||" in content:
        parse_ascii_table_metadata(content)
    else:
        parse_json_table_metadata(log_file_path)

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
    if "Tables" not in SampleMetaData:
        SampleMetaData["Tables"] = {}
    if table_name not in SampleMetaData["Tables"]:
        SampleMetaData["Tables"][table_name] = {}
    existing = SampleMetaData["Tables"][table_name].get(region, {})

    key_schema = table_data.get("KeySchema", [])
    throughput = table_data.get("ProvisionedThroughput", {}) or {}
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

    SampleMetaData["Tables"][table_name][region] = {
        "ItemCount": max_or_existing("ItemCount", table_data.get("ItemCount")),
        "KeySchema": prefer_truthy("KeySchema", key_schema),
        "RCU": max_or_existing("RCU", throughput.get("ReadCapacityUnits")),
        "WCU": max_or_existing("WCU", throughput.get("WriteCapacityUnits")),
        "NumDecreases": max_or_existing("NumDecreases", throughput.get("NumberOfDecreasesToday")),
        "HasLocalSecondaryIndexes": existing.get("HasLocalSecondaryIndexes", False) or bool(table_data.get("LocalSecondaryIndexes")),
        "HasReplicas": existing.get("HasReplicas", False) or bool(table_data.get("Replicas")),
        "StreamsEnabled": existing.get("StreamsEnabled", False) or stream_spec.get("StreamEnabled", False),
        "TableClassSummary": prefer_truthy("TableClassSummary", class_summary.get("TableClass", "")),
    }
    SampleMetaData["Tables"][table_name][region].setdefault("Ops", {})

def _ensure_table_region(meta, table, region):
    tbls = meta.setdefault("Tables", {})
    t = tbls.setdefault(table, {})
    return t.setdefault(region, {})

def _ensure_ops_block(meta, table, region, op):
    reg = _ensure_table_region(meta, table, region)
    ops = reg.setdefault("Ops", {})
    o = ops.setdefault(op, {})
    o.setdefault("Peak", {"Count": None, "P99": None})
    return o

def _update_peak_in_tables(meta, table, region, op, metric, timestamp, value):
    if value is None:
        return
    op_entry = _ensure_ops_block(meta, table, region, op)
    cur = op_entry["Peak"].get(metric)
    if cur is None or cur.get("value") is None or value > cur["value"]:
        op_entry["Peak"][metric] = {"timestamp": timestamp, "value": value}

def _update_peak_in_global(peaks, region, table, op, metric, timestamp, value):
    if value is None:
        return
    reg = peaks.setdefault(metric, {}).setdefault(region, {})
    t = reg.setdefault(table, {})
    cur = t.get(op)
    if cur is None or cur.get("value") is None or value > cur["value"]:
        t[op] = {"timestamp": timestamp, "value": value}

def traverse(request_table=None):
    global SAMPLES, REGIONS, TABLES, BASE_DIR
    REGIONS = set()
    TABLES = set()

    base = os.path.abspath(BASE_DIR)
    if os.path.basename(base).startswith("dynamo_metrics_logs_"):
        SAMPLES = [os.path.basename(base)]
        BASE_DIR = os.path.dirname(base) or "."
    else:
        BASE_DIR = base
        SAMPLES = [
            name for name in os.listdir(BASE_DIR)
            if name.startswith("dynamo_metrics_logs_")
            and os.path.isdir(os.path.join(BASE_DIR, name))
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
    global SampleCountData, SampleP99Data, SampleMetaData

    SampleCountData = {op: [] for op in DDB_OPERATIONS}
    SampleP99Data   = {op: [] for op in DDB_OPERATIONS}
    SampleMetaData  = {op: [] for op in DDB_OPERATIONS}  # keep legacy shape
    SampleMetaData["Tables"] = {}

    Peaks = {"Count": {}, "P99": {}}

    # Parse metadata once per sample (so we can capture changing ItemCount, etc.)
    for sample in SAMPLES:
        parse_table_metadata(os.path.join(BASE_DIR, sample, "table_detailed.log"))

    # Build time-series + peaks (Count)
    for operation in DDB_OPERATIONS:
        for table in TABLES:
            for sample in SAMPLES:
                for region in REGIONS:
                    sample_count_path = os.path.join(BASE_DIR, sample, region, table, operation, "sample_count")
                    if not os.path.isdir(sample_count_path):
                        continue
                    for record in os.listdir(sample_count_path):
                        sample_record = os.path.join(sample_count_path, record)
                        if not os.path.isfile(sample_record):
                            continue
                        try:
                            for ts, val in load_metric_datapoints(sample_record, "Count"):
                                SampleCountData[operation].append({
                                    "timestamp": ts,
                                    "value": val,
                                    "table": table,
                                    "region": region,
                                    "sample": sample,
                                })
                                _update_peak_in_tables(SampleMetaData, table, region, operation, "Count", ts, val)
                                _update_peak_in_global(Peaks, region, table, operation, "Count", ts, val)
                        except Exception as e:
                            print(f"Error reading {sample_record}: {e}")

    # Build time-series + peaks (P99)
    for operation in DDB_OPERATIONS:
        for table in TABLES:
            for sample in SAMPLES:
                for region in REGIONS:
                    p99_path = os.path.join(BASE_DIR, sample, region, table, operation, "p99_latency")
                    if not os.path.isdir(p99_path):
                        continue
                    for record in os.listdir(p99_path):
                        sample_record = os.path.join(p99_path, record)
                        if not os.path.isfile(sample_record):
                            continue
                        try:
                            for ts, val in load_metric_datapoints(sample_record, "P99"):
                                SampleP99Data[operation].append({
                                    "timestamp": ts,
                                    "value": val,
                                    "table": table,
                                    "region": region,
                                    "sample": sample,
                                })
                                _update_peak_in_tables(SampleMetaData, table, region, operation, "P99", ts, val)
                                _update_peak_in_global(Peaks, region, table, operation, "P99", ts, val)
                        except Exception as e:
                            print(f"Error reading {sample_record}: {e}")

    Path("./data").mkdir(parents=True, exist_ok=True)

    with open("./data/sample_count_data.json", "w") as f:
        json.dump(SampleCountData, f, indent=2)

    with open("./data/sample_p99_data.json", "w") as f:
        json.dump(SampleP99Data, f, indent=2)

    with open("./data/sample_metadata.json", "w") as f:
        json.dump(SampleMetaData, f, indent=2)

    with open("./data/sample_peaks.json", "w") as f:
        json.dump(Peaks, f, indent=2)

# ============ Dash UI ============
def run_dash_ui():
    with open("./data/sample_count_data.json") as f:
        sample_count_data = json.load(f)
    with open("./data/sample_p99_data.json") as f:
        sample_p99_data = json.load(f)
    with open("./data/sample_metadata.json") as f:
        sample_metadata = json.load(f)

    # Metadata tab renderer
    def format_key_schema(schema_list):
        parts = []
        for entry in schema_list:
            if entry.get("KeyType") == "HASH":
                parts.append(f"HASH: {entry.get('AttributeName')}")
            elif entry.get("KeyType") == "RANGE":
                parts.append(f"RANGE: {entry.get('AttributeName')}")
        return ", ".join(parts)

    def generate_metadata_tab(tables_metadata: Dict):
        cards = []
        for table_name, region_data in tables_metadata.items():
            region_sections = []
            for region, metadata in region_data.items():
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
                # Add compact peaks summary per op if present
                ops = metadata.get("Ops", {})
                if ops:
                    peak_rows = []
                    for op, info in ops.items():
                        peak = info.get("Peak", {})
                        peak_rows.append({
                            "Operation": op,
                            "PeakCount": peak.get("Count", {}).get("value"),
                            "PeakCountTs": peak.get("Count", {}).get("timestamp"),
                            "PeakP99(ms)": peak.get("P99", {}).get("value"),
                            "PeakP99Ts": peak.get("P99", {}).get("timestamp"),
                        })
                    region_sections.append(html.Div([
                        dash_table.DataTable(
                            columns=[{"name": k, "id": k} for k in display_data],
                            data=[display_data],
                            style_table={"overflowX": "auto"},
                            style_cell={"textAlign": "left", "padding": "5px"},
                            style_header={"fontWeight": "bold"},
                        ),
                        html.Div(style={"height": "10px"}),
                        dash_table.DataTable(
                            columns=[{"name": c, "id": c} for c in ["Operation", "PeakCount", "PeakCountTs", "PeakP99(ms)", "PeakP99Ts"]],
                            data=peak_rows,
                            style_table={"overflowX": "auto"},
                            style_cell={"textAlign": "left", "padding": "5px"},
                            style_header={"fontWeight": "bold"},
                        )
                    ], style={"marginBottom": "40px"}))
                else:
                    region_sections.append(dash_table.DataTable(
                        columns=[{"name": k, "id": k} for k in display_data],
                        data=[display_data],
                        style_table={"overflowX": "auto"},
                        style_cell={"textAlign": "left", "padding": "5px"},
                        style_header={"fontWeight": "bold"},
                    ))
            cards.append(html.Div([html.H4(table_name), html.Div(region_sections)]))
        return html.Div(cards, style={"padding": "20px"})

    # build traces for the two tabs
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
                    mode="lines+markers",
                    name=table,
                    legendgroup=table,
                    showlegend=True,
                ))
            traces_by_op[operation] = traces
        return traces_by_op

    def build_graphs(title_prefix, traces_by_op):
        graphs = []
        # keep order stable-ish
        ops_sorted = sorted(traces_by_op.keys())
        for operation in ops_sorted:
            figure = {
                "data": traces_by_op.get(operation, []),
                "layout": go.Layout(
                    title=f"{title_prefix} - {operation}",
                    xaxis={"title": "Timestamp"},
                    yaxis={"title": "Value"},
                    legend={"itemsizing": "constant"},
                )
            }
            graphs.append(dcc.Graph(id=f"{title_prefix.lower()}-{operation.lower()}", figure=figure))
        return graphs

    tables_metadata = sample_metadata.get("Tables", {})
    count_traces = build_traces(sample_count_data)
    p99_traces = build_traces(sample_p99_data)

    app = Dash(__name__)
    app.layout = html.Div([
        html.H1("DynamoDB Metrics Viewer"),
        dcc.Tabs([
            dcc.Tab(label="Sample Count", children=build_graphs("Sample Count", count_traces)),
            dcc.Tab(label="P99 Latency", children=build_graphs("P99 Latency", p99_traces)),
            dcc.Tab(label="Metadata", children=generate_metadata_tab(tables_metadata)),
        ])
    ])
    app.run(debug=True)

def main(operation, folder=None, table=None):
    if operation == "load":
        global BASE_DIR
        BASE_DIR = folder or "."
        traverse(table)
        process_data()
    elif operation == "UI":
        run_dash_ui()

def parse_args():
    parser = argparse.ArgumentParser(description="DynamoDB metrics loader and Dash visualizer (JSON or CSV)")
    parser.add_argument("operation", type=str, choices=["load", "UI"], help="Process data or launch UI")
    parser.add_argument("--folder", type=str, help="Parent folder containing dynamo_metrics_logs_* directories", default=None)
    parser.add_argument("--table", type=str, help="Optional: limit to a single table")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(args.operation, args.folder, args.table)

# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Graphs the performance of the batch job with HAPI as the source 

This module runs a batch job with HAPI as the source and produces a graph and csv files of the performance of 
the HAPI server, postgres database and the pipeline.

Example usage: 
    python3 graph_pidstat.py \
      --numProc 12  --dataDescription small_dataset \
      --outputParquetPath /tmp/hapi-test/  --outputResultsPath /tmp/hapi-performance/
"""

import sys
import os
import pandas
import subprocess
import argparse
from matplotlib import pyplot
from matplotlib.pyplot import figure
from enum import Enum

parser = argparse.ArgumentParser(
    description="Generate performance graphs for batch mode."
)

parser.add_argument(
    "--numProc",
    type=str,
    required=True,
    help="Provide the number of cores to use in the batch job.",
)

parser.add_argument(
    "--dataDescription",
    type=str,
    required=True,
    help="Provide a description of the data.",
)

parser.add_argument(
    "--outputParquetPath",
    type=str,
    default="/tmp/hapi-test/",
    help="Provide the output path for the parquet files generated by the batch job.",
)

parser.add_argument(
    "--outputResultsPath",
    type=str,
    default="/tmp/hapi-performance/",
    help="Provide the output path for the graphs and csv files.",
)


def get_machine_mem() -> int:
    """
    Gets the RAM of the local machine.

    Args:
      None

    Returns:
      Integer representation of the local machine RAM in GBs
    """
    mem_info = subprocess.run(
        ["grep", "MemTotal", "/proc/meminfo"], stdout=subprocess.PIPE
    ).stdout.split()
    return int(int(mem_info[1]) / 1000000)


def get_machine_cpu() -> int:
    """
    Gets the CPU/number of cores of the local machine.

    Args:
      None

    Returns:
      Integer representation of the number of cores of the local machine
    """
    cpu_info = subprocess.run(["nproc"], stdout=subprocess.PIPE).stdout
    return int(cpu_info)


def monitor_pipeline_pidstat(
    num_proc: int,
    data_description: str,
    output_parquet_path: str,
    output_results_path: str,
) -> None:
    """
    Creates output subdirs and executes bash pipeline monitoring script.

    Args:
      num_proc: Number of processes the batch job is set to run with
      data_description: Description of the dataset
      output_parquet_path: Directory of the output parquet files
      output_results_path: Directory of the output csv files and graphs

    Returns:
      None
    """
    if not os.path.exists(output_results_path + str(data_description) + "/graphs"):
        os.makedirs(output_results_path + str(data_description) + "/graphs")
    if not os.path.exists(output_results_path + str(data_description) + "/tables"):
        os.makedirs(output_results_path + str(data_description) + "/tables")
    if not os.path.exists(output_results_path + str(data_description) + "/raw"):
        os.makedirs(output_results_path + str(data_description) + "/raw")
    subprocess.call(
        [
            "sh",
            "./monitor_pipeline.sh",
            num_proc,
            data_description,
            output_parquet_path,
            output_results_path,
        ]
    )


def make_pidstat_tables(file_name: str, type: str) -> pandas.DataFrame:
    """
    Creates pandas tables from raw pidstat data.

    Args:
      file_name: Name of the .txt file containing raw pidstat output
      type: Type of the resource relevant to the table (Pipeline, Server, or DB)

    Returns:
      Pandas table with CPU, memory and I/O usage info for the resource.
    """

    # Parse the raw .txt file into a pandas dataframe
    total_usage = []
    seconds_elapsed = 0

    # Expected format of each transformation:
    # 1. Read a line in the raw file
    # 2. Transforms a line in the raw file to a list of strings.
    # 3. Delete all elements before the %CPU column index
    # 4. Delete all elements after the kB_wr/s column index
    # 5. Delete all elements the %CPU and %MEM columns index
    # 6. Insert the time elapsed (s) at the front of the list
    with open(file_name) as f:
        for line in f:
            usage = []
            if line[0].isdigit():
                usage = line.split()

                if type == "DB" and usage[-3] != "hapi":
                    continue

                del usage[:8]
                del usage[9:]
                del usage[1:6]
                usage.insert(0, seconds_elapsed)
                total_usage.append(usage)
            elif line[0] == "#":
                seconds_elapsed += 1

    total_usage_df = pandas.DataFrame(
        total_usage,
        columns=[
            "Seconds Elapsed",
            type + " %CPU",
            type + " MEM",
            type + " kBs Read / Second",
            type + " kBs Written / Second",
        ],
    )
    total_usage_df[type + " MEM"] = total_usage_df[type + " MEM"].apply(
        lambda x: float(x) / 100 * get_machine_mem()
    )

    # Cast types of all columns
    for col in total_usage_df:
        total_usage_df[col] = total_usage_df[col].astype(float)

    # Summation by the second for the database table
    if type == "DB":
        total_usage_df["DB %CPU"] = total_usage_df.groupby(["Seconds Elapsed"])[
            "DB %CPU"
        ].transform(sum)
        total_usage_df["DB MEM"] = total_usage_df.groupby(["Seconds Elapsed"])[
            "DB MEM"
        ].transform(sum)
        total_usage_df["DB kBs Written / Second"] = total_usage_df.groupby(
            ["Seconds Elapsed"]
        )["DB kBs Written / Second"].transform(sum)
        total_usage_df["DB kBs Read / Second"] = total_usage_df.groupby(
            ["Seconds Elapsed"]
        )["DB kBs Read / Second"].transform(sum)
        total_usage_df = total_usage_df.drop_duplicates(subset=["Seconds Elapsed"])

    total_usage_df[total_usage_df < 0] = 0

    return total_usage_df


def graph_pidstat(
    num_proc: int, data_description: str, output_results_path: str
) -> None:
    """
    Generates graphs for server, pipeline and database resource usage information at the specified output directory.

    Args:
      num_proc: Number of processes the batch job is set to run with
      data_description: Description of the dataset
      output_results_path: Directory of the output csv files and graphs

    Returns:
      None
    """

    # Create pandas dataframes for all resource types and merge into one big dataframe
    pipeline_df = make_pidstat_tables(
        output_results_path
        + str(data_description)
        + "/raw/pipeline_stats_"
        + str(num_proc)
        + "_proc.txt",
        "Pipeline",
    )
    server_df = make_pidstat_tables(
        output_results_path
        + str(data_description)
        + "/raw/server_stats_"
        + str(num_proc)
        + "_proc.txt",
        "Server",
    )
    db_df = make_pidstat_tables(
        output_results_path
        + str(data_description)
        + "/raw/database_stats_"
        + str(num_proc)
        + "_proc.txt",
        "DB",
    )
    all_stats_df = pandas.merge(pipeline_df, server_df, on="Seconds Elapsed")
    all_stats_df = pandas.merge(all_stats_df, db_df, on="Seconds Elapsed")

    # Generate graphs with resource usage information
    figure(figsize=(32, 20), dpi=80)
    pyplot.subplot(3, 1, 1)
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Pipeline %CPU"],
        "-o",
        label="Pipeline",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Server %CPU"],
        "-o",
        label="Server",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"], all_stats_df["DB %CPU"], "-o", label="DB"
    )
    pyplot.ylabel("%CPU Usage", fontsize=14)
    pyplot.legend()
    pyplot.title("Pipeline vs Server vs Database %CPU Usage", fontsize=14)

    pyplot.subplot(3, 1, 2)
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Pipeline MEM"],
        "-o",
        label="Pipeline",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Server MEM"],
        "-o",
        label="Server",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"], all_stats_df["DB MEM"], "-o", label="DB"
    )
    pyplot.ylabel("MEM Usage (GB)", fontsize=14)
    pyplot.legend()
    pyplot.title("Pipeline vs Server vs Database MEM Usage", fontsize=14)

    pyplot.subplot(3, 1, 3)
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Pipeline kBs Written / Second"],
        "-o",
        label="Pipeline kBs Written / s",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Server kBs Written / Second"],
        "-o",
        label="Server kBs Written / s",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["DB kBs Written / Second"],
        "-o",
        label="DB kBs Written / s",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Pipeline kBs Read / Second"],
        "-o",
        label="Pipeline kBs Read / s",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["Server kBs Read / Second"],
        "-o",
        label="Server kBs Read / s",
    )
    pyplot.plot(
        all_stats_df["Seconds Elapsed"],
        all_stats_df["DB kBs Read / Second"],
        "-o",
        label="DB kBs Read / s",
    )
    pyplot.ylabel("I/O Usage (kBs/s)", fontsize=14)
    pyplot.xlabel("Seconds Elapsed", fontsize=14)
    pyplot.title("Pipeline vs Server vs DB I/O Usage", fontsize=14)

    pyplot.text(
        0.005,
        0.645,
        "Run Summary",
        fontsize=14,
        weight="bold",
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.62,
        "Total time: " + str(int(all_stats_df["Seconds Elapsed"].max())) + " s",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.60,
        "Avg. pipeline %CPU: " + str(round(all_stats_df["Pipeline %CPU"].mean(), 1)),
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.58,
        "Avg. server %CPU: " + str(round(all_stats_df["Server %CPU"].mean(), 1)),
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.56,
        "Avg. database %CPU: " + str(round(all_stats_df["DB %CPU"].mean(), 1)),
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.54,
        "Avg. pipeline MEM: "
        + str(round(all_stats_df["Pipeline MEM"].mean(), 1))
        + " GB",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.52,
        "Avg. server MEM: " + str(round(all_stats_df["Server MEM"].mean(), 1)) + " GB",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.5,
        "Avg. database MEM: " + str(round(all_stats_df["DB MEM"].mean(), 1)) + " GB",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.48,
        "Avg. pipeline read: "
        + str(round(all_stats_df["Pipeline kBs Read / Second"].mean(), 1))
        + " kBs/s",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.46,
        "Avg. server read: "
        + str(round(all_stats_df["Server kBs Read / Second"].mean(), 1))
        + " kBs/s",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.44,
        "Avg. database read: "
        + str(round(all_stats_df["DB kBs Read / Second"].mean(), 1))
        + " kBs/s",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.42,
        "Avg. pipeline write: "
        + str(round(all_stats_df["Pipeline kBs Written / Second"].mean(), 1))
        + " kBs/s",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.40,
        "Avg. server write: "
        + str(round(all_stats_df["Server kBs Written / Second"].mean(), 1))
        + " kBs/s",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.text(
        0.005,
        0.38,
        "Avg. database write: "
        + str(round(all_stats_df["DB kBs Written / Second"].mean(), 1))
        + " kBs/s",
        fontsize=12,
        transform=pyplot.gcf().transFigure,
    )
    pyplot.suptitle(
        "Resource Usage of Batch Pipeline with HAPI as the Source on Local Machine ("
        + str(get_machine_cpu())
        + " "
        + "CPU, "
        + str(get_machine_mem())
        + " GB RAM, "
        + str(num_proc)
        + " proc, "
        + str(data_description)
        + " data)",
        fontsize=16,
        weight="bold",
    )
    pyplot.legend()

    # Save the graph and csv files
    pyplot.savefig(
        output_results_path
        + str(data_description)
        + "/graphs/resource_usage_"
        + str(num_proc)
        + "_proc.png"
    )
    all_stats_df.to_csv(
        output_results_path
        + str(data_description)
        + "/tables/resource_usage_"
        + str(num_proc)
        + "_proc.csv",
        encoding="utf-8",
        index=False,
    )


if __name__ == "__main__":
    args = parser.parse_args()
    monitor_pipeline_pidstat(
        args.numProc,
        args.dataDescription,
        args.outputParquetPath,
        args.outputResultsPath,
    )
    graph_pidstat(args.numProc, args.dataDescription, args.outputResultsPath)

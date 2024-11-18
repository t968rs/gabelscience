import json
import os
import dask.dataframe as dd
from src.d00_utils.system import get_system_memory


def determine_partition_size(file_size_gb, num_rows, num_vertices):
    # Basic heuristic for partitioning
    base_partitions = 2 * os.cpu_count()
    memory_gb = get_system_memory()

    # Factors to adjust the number of partitions
    size_factor = file_size_gb / memory_gb
    row_factor = num_rows / 1e6  # Assume 1 million rows per partition
    vertex_factor = num_vertices / 1e7  # Assume 10 million vertices per partition

    # Calculate the number of partitions
    num_partitions = int(base_partitions * (1 + size_factor + row_factor + vertex_factor))
    return max(num_partitions, base_partitions)  # Ensure at least base_partitions

def determine_chunk_size(num_rows, complexity_factor=1.0):
    # Estimate the number of partitions based on the number of rows and complexity
    target_rows_per_partition = 200_000  # Adjust this based on your data complexity
    num_partitions = max(1, int(num_rows / target_rows_per_partition))

    # Adjust based on system memory
    total_memory_gb = get_system_memory()
    estimated_memory_per_row_mb = complexity_factor * 0.01  # Adjust this based on your data complexity
    estimated_partition_memory_mb = target_rows_per_partition * estimated_memory_per_row_mb
    max_partitions_by_memory = int((total_memory_gb * 1024) / estimated_partition_memory_mb)

    # Choose the smaller number of partitions to ensure memory constraints are respected
    num_partitions = min(num_partitions, max_partitions_by_memory)

    # Calculate the chunk size
    chunk_size = max(1, int(num_rows / num_partitions))
    return chunk_size


class OptimizingDask:
    def __init__(self, df):
        self.df = df

    def optimize(self, new_partition_size=None, system_memory=32):
        if not new_partition_size:
            new_partition_size = determine_partition_size(
                self.get_memory_usage() / 1e9,
                self.df.shape[0],
                self.df.shape[1],
            )
        self.df = self.df.repartition(npartitions=2)
        return self.df

    def get_memory_usage(self):
        return self.df.memory_usage(deep=True).sum().compute()

    def get_partition_size(self):
        return self.df.map_partitions(lambda x: x.memory_usage(deep=True).sum()).compute()

    def get_partition_count(self):
        return self.df.npartitions

    def get_partition(self, partition_id):
        return self.df.get_partition(partition_id).compute()

    def get_partition_shape(self, partition_id):
        return self.df.get_partition(partition_id).shape

    def get_partition_memory_usage(self, partition_id):
        return self.df.get_partition(partition_id).memory_usage(deep=True).sum().compute()

    def get_partition_columns(self, partition_id):
        return self.df.get_partition(partition_id).columns

    def get_partition_dtypes(self, partition_id):
        return self.df.get_partition(partition_id).dtypes

    def get_partition_info(self, partition_id):
        return {
            "shape": self.get_partition_shape(partition_id),
            "memory_usage": self.get_partition_memory_usage(partition_id),
            "columns": self.get_partition_columns(partition_id),
            "dtypes": self.get_partition_dtypes(partition_id)
        }

    def get_partition_info_all(self):
        return {i: self.get_partition_info(i) for i in range(self.get_partition_count())}

    def get_partition_info_all_summary(self):
        return {
            "total_memory_usage": self.get_memory_usage(),
            "partition_size": self.get_partition_size(),
            "partition_count": self.get_partition_count(),
            "partition_info": self.get_partition_info_all()
        }

    def get_partition_info_all_summary_json(self):
        return json.dumps(self.get_partition_info_all_summary(), indent=4)


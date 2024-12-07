import json

class DynamicConfig:
    
# def apply_iceberg_recommendations(config: Dict[str, str], iceberg_meta: Dict[str, Any], dataset_size_gb: float) -> Dict[str, str]:
#     if iceberg_meta["snapshot_count"] > 40:
#         config.update({
#             "spark.sql.iceberg.expire.snapshots.enabled": "true",
#             "spark.sql.iceberg.expire.snapshots.retention-interval": "7d"
#         })
#     else:
#         config.update({
#             "spark.sql.iceberg.expire.snapshots.enabled": "true",
#             "spark.sql.iceberg.expire.snapshots.retention-interval": "30d"
#         })
        
#     if iceberg_meta["table_size_gb"] > 300:
#         config.update({
#             "spark.sql.iceberg.compaction.enabled": "true",
#             "spark.sql.iceberg.compaction.target-file-size-bytes": str(512 * 1024 * 1024),
#             "spark.sql.iceberg.compaction.parallelism": "8",
#             "spark.sql.iceberg.merge-snapshot.parallelism": "8"
#         })
#     else:
#         config.update({
#             "spark.sql.iceberg.compaction.enabled": "true",
#             "spark.sql.iceberg.compaction.target-file-size-bytes": str(256 * 1024 * 1024),
#             "spark.sql.iceberg.compaction.parallelism": "4",
#             "spark.sql.iceberg.merge-snapshot.parallelism": "4"
#         })
        
#     return config

# def get_emr_serverless_app_info(application_id: str, region_name: str = "us-east-1") -> Dict[str, Any]:
#     client = boto3.client("emr-serverless", region_name=region_name)
#     response = client.get_application(applicationId=application_id)
#     app_info = response.get("application", {})
#     max_cap = app_info.get("maximumCapacity", {})
#     max_cpu_str = max_cap.get("cpu", "100")
#     max_memory_str = max_cap.get("memory", "512")
#     max_cpu = int(max_cpu_str.split(":")[1])
#     max_memory_gb = int(max_memory_str.split(":")[1])
#     return {
#         "max_cpu": max_cpu,
#         "max_memory_gb": max_memory_gb
#     }

    @classmethod
    def determine_cores(cls, dataset_size_gb: float) -> int:
        if dataset_size_gb < 1:
            return 2
        elif dataset_size_gb < 10:
            return 2
        elif dataset_size_gb < 100:
            return 4
        elif dataset_size_gb < 500:
            return 8
        elif dataset_size_gb < 1000:
            return 12
        else:
            return 16

    @classmethod   
    def determine_driver_settings(cls, dataset_size_gb: float, max_memory_per_executor_gb: int) -> (int, int):
        if dataset_size_gb < 1:
            driver_memory_gb = 2
            driver_cores = 1
        elif dataset_size_gb < 10:
            driver_memory_gb = min(4, max_memory_per_executor_gb)
            driver_cores = 2
        elif dataset_size_gb < 100:
            driver_memory_gb = min(8, max_memory_per_executor_gb)
            driver_cores = 2
        elif dataset_size_gb < 500:
            driver_memory_gb = min(8, max_memory_per_executor_gb)
            driver_cores = 2
        else:
            driver_memory_gb = min(16, max_memory_per_executor_gb)
            driver_cores = 4
        return driver_memory_gb, driver_cores

    @classmethod
    def determine_disk_size(cls, dataset_size_gb: float) -> int:
        if dataset_size_gb < 20:
            return 20
        elif dataset_size_gb < 40:
            return 50
        elif dataset_size_gb < 100:
            return 100
        elif dataset_size_gb < 500:
            return 200
        else:
            return 200
    @classmethod
    def determine_memory_overhead_factor(cls, dataset_size_gb: float) -> float:
        if dataset_size_gb < 1:
            return 0.1
        elif dataset_size_gb < 10:
            return 0.15
        elif dataset_size_gb < 100:
            return 0.2
        elif dataset_size_gb < 500:
            return 0.25
        else:
            return 0.3
        
    @classmethod
    def recommend_spark_config(
        cls,
        dataset_size_gb: float,
        job_type: str = "batch",
        optimization_goal: str = "cost",
        avg_file_size_mb: int = 500,
        max_executors: int = 50,
        max_memory_per_executor_gb: int = 120,
        emr_application_id: str = None,
        num_instances: int = 10
    ):
        
        # Memory for executors
        additional_memory_gb = int(dataset_size_gb // 10)
        executor_memory_gb = min(2 + additional_memory_gb, max_memory_per_executor_gb)
        
        # Cores executors
        base_cores = cls.determine_cores(dataset_size_gb)
        # Driver
        driver_memory_gb, driver_cores = cls.determine_driver_settings(dataset_size_gb, max_memory_per_executor_gb)
        
        # Particiones
        base_partitions = max(20, int(dataset_size_gb * 20))
        desired_partitions_by_file_size = max(50, int((avg_file_size_mb / 256) * base_partitions))
        shuffle_partitions = max(base_partitions, desired_partitions_by_file_size)
        
        if dataset_size_gb > 500:
            shuffle_partitions = max(shuffle_partitions, 2000)
            
        # Disco
        disk_gb = cls.determine_disk_size(dataset_size_gb)
        memory_overhead_factor = cls.determine_memory_overhead_factor(dataset_size_gb)
        
        config = {
            "spark.dynamicAllocation.enabled": 'true',
            "spark.dynamicAllocation.initialExecutors": '2',
            "spark.dynamicAllocation.minExecutors": '2',
            "spark.dynamicAllocation.maxExecutors": '14'
        }
        
        if job_type == "batch":
            config["spark.sql.autoBroadcastJoinThreshold"] = "-1"
        elif job_type == "interactive":
            shuffle_partitions = min(shuffle_partitions, 200)
            
        if optimization_goal == "throughput":
            config["spark.sql.parquet.compression.codec"] = "snappy"
            if num_instances < 10:
                config["spark.dynamicAllocation.maxExecutors"] = str(min(max_executors, 20))
        elif optimization_goal == "cost":
            if num_instances > 10:
                config["spark.dynamicAllocation.maxExecutors"] = str(min(max_executors, 30))
            else:
                config["spark.dynamicAllocation.maxExecutors"] = str(min(max_executors, 15))
                
        config.update({
            "spark.executor.memory": f"{executor_memory_gb}g",
            "spark.executor.cores": str(base_cores),
            "spark.driver.memory": f"{driver_memory_gb}g",
            "spark.driver.cores": str(driver_cores),
            "spark.shuffle.partitions": str(shuffle_partitions),
            # "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
            # "spark.sql.iceberg.merge-snapshot.enabled": "true",
            
            # EMR Serverless:
            "spark.emr-serverless.executor.disk": f"{disk_gb}G",
            "spark.emr-serverless.driver.disk": f"{disk_gb}G",
            "spark.emr-serverless.memoryOverheadFactor": str(memory_overhead_factor)
        })
        
        if dataset_size_gb > 100 and job_type == "batch":
            config.update({
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.shuffle.targetPostShuffleInputSize": "256m",
                "spark.sql.parquet.enableVectorizedReader": "true"
            })
        elif dataset_size_gb < 1:
            config["spark.sql.adaptive.enabled"] = "false"
            
        # InIntegration with EMR Serverless API
        # if emr_application_id is not None:
        #     emr_info = get_emr_serverless_app_info(emr_application_id, region_name)
        #     max_cpu = emr_info["max_cpu"]
        #     max_mem = emr_info["max_memory_gb"]
        #     possible_max_executors_by_memory = max_mem // executor_memory_gb
        #     new_max_executors = min(int(config["spark.dynamicAllocation.maxExecutors"]), possible_max_executors_by_memory)
        #     possible_max_executors_by_cpu = max_cpu // base_cores
        #     new_max_executors = min(new_max_executors, possible_max_executors_by_cpu)
        #     config["spark.dynamicAllocation.maxExecutors"] = str(new_max_executors)
            
        
        return config

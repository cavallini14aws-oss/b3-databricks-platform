import time

from data_platform.core.context import get_context


def build_model_artifact_path(
    model_name: str,
    model_version: str,
    project: str = "template",
    use_catalog: bool = False,
) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)
    return f"{ctx.model_artifact_base_path}/{model_name}/{model_version}"


def _exists_via_hadoop_fs(spark, path_str: str) -> bool:
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = jvm.org.apache.hadoop.fs.Path(path_str)
        return fs.exists(path)
    except Exception:
        return False


def _exists_via_dbutils(path_str: str) -> bool:
    try:
        from dbruntime.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return False

        dbutils = DBUtils(spark)
        dbutils.fs.ls(path_str)
        return True
    except Exception:
        return False


def artifact_exists(
    spark,
    artifact_path: str,
    retries: int = 8,
    sleep_seconds: int = 2,
) -> bool:
    candidate_paths = [artifact_path]

    if artifact_path.startswith("/Volumes/"):
        candidate_paths.append(f"dbfs:{artifact_path}")

    for _ in range(retries):
        for candidate in candidate_paths:
            if _exists_via_hadoop_fs(spark, candidate):
                return True
            if _exists_via_dbutils(candidate):
                return True
        time.sleep(sleep_seconds)

    return False

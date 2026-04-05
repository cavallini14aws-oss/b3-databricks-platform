from data_platform.core.context import get_context


def build_model_artifact_path(
    model_name: str,
    model_version: str,
    project: str = "template",
    use_catalog: bool = False,
) -> str:
    ctx = get_context(project=project, use_catalog=use_catalog)
    return f"{ctx.model_artifact_base_path}/{model_name}/{model_version}"


def artifact_exists(
    spark,
    artifact_path: str,
) -> bool:
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = jvm.org.apache.hadoop.fs.Path(artifact_path)
        return fs.exists(path)
    except Exception:
        return False

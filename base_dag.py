from airflow.providers.ssh.operators.ssh import SSHOperator
from common import base_dag


class MeasureSTGDag(base_dag.BaseDag):
    def create_dag_flow(self, task_params: dict = None):
        measures_task = SSHOperator(
            task_id="download-measures",
            ssh_conn_id=task_params["measures_task"]["ssh_conn_id"],
            cmd_timeout=20 * 60,
            command=(
                "docker exec dataion-pipeline python stg_devices/pipeline_download_measures.py "
                f"--file_paths {task_params['measures_task']['file_paths']} "
                "--end_date \"{{ data_interval_end.strftime('%Y/%m/%d %H:00:00') }}\" "
                "--tz Europe/Madrid "
                f"--protocol {task_params['measures_task']['protocol']} "
            ),
        )

        measures_task  # noqa: B018

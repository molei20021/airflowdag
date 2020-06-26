# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example DAG demonstrating Kubernetes Pod Operator."""

# [START composer_kubernetespodoperator]
import datetime
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

def gen_kubernets_operator_python(input_path, output_path, exec_file_name, requirement_file_path, task_name):
    return kubernetes_pod_operator.KubernetesPodOperator(
        task_id=task_name,
        name=task_name,
        namespace='airflowsys',
        in_cluster=True,
        env_vars={'INPUT_PATH':str(input_path), 'OUTPUT_PATH':output_path, 'EXEC_FILE_NAME':str(exec_file_name), 'REQUIREMENT_FILE_PATH':str(requirement_file_path)},
        is_delete_operator_pod=True,
        image="reg.sfai.ml/library/airflow/python:3.7-slim-buster")

# If a Pod fails to launch, or has an error occur in the container, Airflow
# will show the task as failed, as well as contain all of the task logs
# required to debug.
with models.DAG(
        dag_id='kubernetes_pod_pip_install_example',
        schedule_interval='@once',
        start_date=YESTERDAY) as dag:
    # Only name, namespace, image, and task_id are required to create a
    # KubernetesPodOperator. In Cloud Composer, currently the operator defaults
    # to using the config file found at `/home/airflow/composer_kube_config if
    # no `config_file` parameter is specified. By default it will contain the
    # credentials for Cloud Composer's Google Kubernetes Engine cluster that is
    # created upon environment creation.

    # [START composer_kubernetespodoperator_minconfig]
    kubernetes_min_pod = gen_kubernets_operator_python("/usr/local/airflow/jupyter/01374862/manzeng/input", "/usr/local/airflow/jupyter/01374862/output", "test.py","/usr/local/airflow/jupyter/01374862/input", "task_test")

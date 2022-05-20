from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSensorTimeout
from datetime import datetime
default_args = {
'start_date': datetime(2021, 1, 1)
}
def _done():
pass
def _partner_a():
return False
def _partner_b():
return True
def _failure_callback(context):
if isinstance(context['exception'], AirflowSensorTimeout):
print(context)
print("Sensor timed out")
with DAG('my_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
partner_a = PythonSensor(
task_id='partner_a',
poke_interval=120,
timeout=10,
mode="reschedule",
python_callable=_partner_a,
on_failure_callback=_failure_callback,
soft_fail=True
)
partner_b = PythonSensor(
task_id='partner_b',
poke_interval=120,
timeout=10,
mode="reschedule",
python_callable=_partner_b,
on_failure_callback=_failure_callback,
soft_fail=True
)
done = PythonOperator(
task_id="done",
python_callable=_done,
trigger_rule='none_failed_or_skipped'
)
[partner_a, partner_b] >> done

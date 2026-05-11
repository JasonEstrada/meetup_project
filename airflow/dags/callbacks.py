from airflow.sdk import Variable
from airflow.providers.slack.hooks.slack import SlackHook

# Configuration variables
SLACK_CONN_ID = Variable.get("SLACK_CONN_ID", default="slack_default")
SLACK_CHANNEL = Variable.get("SLACK_CHANNEL", default="alertas-airflow")

def _build_message(context, exito: bool):
    dag_id = context["dag"].dag_id
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = task_instance.run_id

    emoji = "✅" if exito else "❌"
    estado = "completed" if exito else "failed"

    return (
        f"{emoji} *DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*State:* {estado}\n"
        f"*Run ID:* `{run_id}`\n"
    )


def on_success(context):
    hook = SlackHook(slack_conn_id=SLACK_CONN_ID)
    hook.call(
        api_method="chat.postMessage",
        json={
            "channel": SLACK_CHANNEL,
            "text": _build_message(context, exito=True),
        },
    )


def on_failure(context):
    hook = SlackHook(slack_conn_id=SLACK_CONN_ID)
    hook.call(
        api_method="chat.postMessage",
        json={
            "channel": SLACK_CHANNEL,
            "text": _build_message(context, exito=False),
        },
    )
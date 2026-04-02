import os

base_log_folder = os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")

loki_host = "http://loki:3100"
frontend_url = os.environ.get("AIRFLOW__LOGGING__LOKI_FRONTEND_URL", "http://localhost:3000")

# Fulfill Airflow 3 SDK dependency for remote logging IO
try:
    from airflow.providers.grafana.loki.log.loki_task_handler import LokiRemoteLogIO
    import airflow.logging_config as alc
    alc.REMOTE_TASK_LOG = LokiRemoteLogIO(
        base_log_folder=base_log_folder,
        host=loki_host,
        delete_local_copy=False,
    )
except ImportError as e:
    pass

LOGGING_CONFIG = {
    'disable_existing_loggers': False,
    'filters': {
        'mask_secrets_core': {
            'class': 'airflow.utils.log.secrets_masker.SecretsMasker'
        }
    },
    'formatters': {
        'airflow': {
            'format': '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'filters': ['mask_secrets_core']
        },
        'task': {
            'base_log_folder': base_log_folder,
            'class': 'airflow.providers.grafana.loki.log.loki_task_handler.LokiTaskHandler',
            'host': loki_host,
            'frontend': frontend_url,
            'filters': ['mask_secrets_core']
        }
    },
    'loggers': {
        'airflow.task': {
            'filters': ['mask_secrets_core'],
            'handlers': ['task'],
            'level': 'INFO',
            'propagate': True
        },
        'flask_appbuilder': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': True
        }
    },
    'root': {
        'filters': ['mask_secrets_core'],
        'handlers': ['console'],
        'level': 'INFO'
    },
    'version': 1
}

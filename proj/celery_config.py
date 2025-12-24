from celery import Celery

celery_app = Celery('proj',
             broker='amqp://localhost//',
             backend='rpc://localhost//',
             include=['proj.tasks'],
             # task_cls= 'proj.app_celery:AppCelery'

             )

# Optional configuration, see the application user guide.
celery_app.conf.update(
    result_expires=3600,
)


# Start the worker in normal terminal using from the root path -> celery -A proj.celery_config worker --loglevel=INFO
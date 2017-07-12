from celery import Celery
from subprocess import call

app = Celery('call_distcp', backend = 'amqp://', broker = 'amqp://')

@app.task
def async_call_distcp(source_data_block_url, dis_data_block_url):
    ret_val = call(["hadoop", "distcp", source_data_block_url, dis_data_block_url])
    return ret_val

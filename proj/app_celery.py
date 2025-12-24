from proj.celery_config import celery_app
from proj.tasks import mul,call_http

if __name__ == '__main__':


    print("starting the job")
    mul.delay(4,4)

# start the workers in the termianl


from celery import Task, shared_task


# class AppCelery(Task):
#
#     # def __init__(self):
#     #     print("init app celery task")
#     #     self.users = {'george': 'password'}
#
#     def hello(self):
#         print("hello app celery task")
#
#     def say_hello(self):
#         print("say hello app celery task")
#     # @shared_task
#     # @celery_app.task(bind=True)
#     def run(self, username, password):
#         try:
#             print(f"run app celery task {username}:{password}")
#             return self.users[username] == password
#         except KeyError:
#             return False
#
# # add_app = app.tasks[AppCelery.name]
# if __name__ == '__main__':
#     apc = AppCelery()
#     apc.delay("geroge", "abv")
    # celery_app.tasks.register(AppCelery())
    # apc.delay('george', 'password')
    #
    # result = call_http.delay("https://dummyjson.com/products/")
    # result = result.get()
    # print(result)
    # print("waiting for results ")
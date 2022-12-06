from datetime import datetime
import time

from database import logger, track

class Register(type):
    def __new__(self, name, bases, attrs):
        klass = type.__new__(self, name, bases, attrs)
        Scheduler.add(klass, name, bases, attrs)
        return klass


class Scheduler():
    tasks = []

    @classmethod
    def add(self, cls, name, bases, attrs):

        if hasattr(cls, 'needUpdateDays') and ('name' in attrs):

            if attrs["name"] in track:
                last_record = track[attrs["name"]]
            else:
                last_record = datetime.fromisoformat(cls.schedule_start_date)

            index = 0

            for param in cls.needUpdateDays(last_record):

                logger.blue('SCHEDULER ADD: ' + name + ' ' + str(param))

                self.tasks.insert(index, 
                    (lambda p: lambda: cls().update(**p)) (param)
                )

                index += 2


    @classmethod
    def run(self):
        t = time.time()
        for task in self.tasks:
            task()

            if(time.time() - t > 5):
                logger.yellow('Wait 2s')
                time.sleep(5)
                t = time.time()
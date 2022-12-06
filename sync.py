import sys
import os
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))


from database.get_date import sync
sync()

import dataset
from engine.register import Scheduler

Scheduler.run()
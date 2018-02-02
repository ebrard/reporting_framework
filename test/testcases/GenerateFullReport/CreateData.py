import dataset
from faker import Faker
from dateutil.relativedelta import relativedelta
import datetime

db = dataset.connect('sqlite:////Users/manu/Documents/ReportingFramework/test/data.db')

table = db['customer']

fake = Faker()

for i in range(0, 100):
    birth_date = fake.date_between(start_date="-60y", end_date="today")
    age = relativedelta(datetime.datetime.now(), birth_date).years
    table.insert(dict(id=i,
                      first_name=fake.first_name(),
                      last_name=fake.last_name(),
                      age=age,
                      birthdate=birth_date))

db.commit()

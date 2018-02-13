import dataset
from faker import Faker
from dateutil.relativedelta import relativedelta
import datetime
import sys
import pickle as pickle
import random

file_created = "/Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/created.pickle"
file_status_path = "/Users/manu/Documents/ReportingFramework/test/testcases/GenerateDeltaReport/file_status.dat"

nb_customers = int(sys.argv[2])

db = dataset.connect('sqlite:////Users/manu/Documents/ReportingFramework/test/data.db')

table = db['customer']

fake = Faker()

fake_data_set = {}

if str(sys.argv[1]) == "create":

    with open(file_created, 'wb') as f:
        for i in range(0, nb_customers):
            birth_date = fake.date_between(start_date="-60y", end_date="today")
            age = relativedelta(datetime.datetime.now(), birth_date).years

            faked_data = dict(id=i,
                              first_name=fake.first_name(),
                              last_name=fake.last_name(),
                              age=age,
                              birthdate=birth_date)

            fake_data_set[str(i)] = faked_data
            table.insert(faked_data)

        pickle.dump(fake_data_set, f)

    db.commit()


if str(sys.argv[1]) == "modify":

    file_status = open(file_status_path, 'w')

    with open(file_created, 'rb') as f_created:
        initial_data = pickle.load(f_created)

        for i in range(0, nb_customers):

            dice = random.randint(0, 100)

            if dice < 20:
                # delete
                file_status.write(str(i) + " D\n")

                db.query('delete from customer where id = %s' % i)

            if dice > 80:
                # Update
                file_status.write(str(i) + " U\n")
                birth_date = fake.date_between(start_date="-60y", end_date="today")
                age = relativedelta(datetime.datetime.now(), birth_date).years

                faked_data = dict(id=i,
                                  first_name=fake.first_name(),
                                  last_name=fake.last_name(),
                                  age=age,
                                  birthdate=birth_date)
                table.update(faked_data, ["id"])
                print("Customer with id " + str(i))

            if 20 <= dice <= 80:
                file_status.write(str(i) + " S\n")

        db.commit()
        file_status.close()

    file_status.close()


from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime
import time

MONGODB_CONN_STR='...'

conn = getattr(MongoClient(MONGODB_CONN_STR), 'emodi')
conn_s = getattr(MongoClient(MONGODB_CONN_STR), 'emodi_management')

while 1:
    date = datetime.datetime.utcnow()
    old_date = date - datetime.timedelta(minutes=3)

    inserted = []

    for homework in conn_s.homework.find({}):
        homework_id = homework.get('_id')
        valid_thru = homework.get('valid_thru')
        valid_from = homework.get('valid_from')

        if old_date < valid_from < date:
            school_side = []
            emodi_side = []
            misses = []

            for user in conn_s.homework_user_rel.find({"homework_id": str(homework_id)}):
                student = user.get('user_id')
                try:
                    student_id = conn_s.user.find_one({"_id": ObjectId(student)}).get('emodi_user_id')
                except AttributeError:
                    continue

                school_side.append(student_id)

            for user in conn.homework_user_rel.find({"homework_id": str(homework_id)}):
                student = user.get('user_id')
                emodi_side.append(student)

            for student in school_side:
                if student not in emodi_side:
                    misses.append(student)

            print(len(emodi_side), len(misses), misses, homework_id)

            for student in misses:
                if date > valid_thru:
                    xCompleted = conn.homework_user_rel.insert_one({
                        "status": "COMPLETED",
                        "user_id": student,
                        "homework_id": str(homework_id)
                    })
                    print(xCompleted.inserted_id)
                    inserted.append(xCompleted.inserted_id)
                elif valid_from < date < valid_thru:
                    xActive = conn.homework_user_rel.insert_one({
                        "status": "ACTIVE",
                        "user_id": student,
                        "homework_id": str(homework_id)
                    })
                    print(xActive.inserted_id)
                    inserted.append(xActive.inserted_id)
        else:
            continue

        print(inserted, file=open('{}.txt'.format(str(homework_id)), 'w'))

    print("homework check done {}".format(date))
    time.sleep(180)
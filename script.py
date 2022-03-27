import csv
import os
import sys
from datetime import datetime
import psycopg2

def execute_sql(file_path):
    # print("Executing SQL: {}".format(file_path))
    try:
        connection = psycopg2.connect(user="",
                                    password="",
                                    host="",
                                    port="5432",
                                    database="",
                                    options="-c search_path=")
        cursor = connection.cursor()
        # postgreSQL_select_Query = "select 1;"

        cursor.execute(open(file_path, "r").read())
        # cursor.execute(postgreSQL_select_Query)
        rows = cursor.fetchall()        
        return rows

    except(Exception, psycopg2.Error) as error:
        error_message = "Error: {}".format(str(error))
        # print(error_message)
        return error_message

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()

def parse_responses_to_file(file_name):
    print("Reading file: ", file_name)

    folder_prefix = "{}_sqls".format(file_name)

    os.mkdir(folder_prefix)

    with open(file_name) as fd:
        reader = csv.reader(fd, delimiter="\t", quotechar='"')
        next(reader) # skip header
        for row in reader:
            date = row[0]
            email = row[1]
            query = row[2]

            formatted_date = datetime.strptime(date, "%d/%m/%Y %H:%M:%S")
            formatted_date_str = formatted_date.strftime("%Y%m%d_%H%M%S")

            new_file = os.path.join(folder_prefix, formatted_date_str + "_" + email.replace("@keyvalue.systems", "") + ".sql")
            
            with open(new_file, 'w') as f:
                f.write(query)

parse_responses_to_file(sys.argv[1])

correct_answer = execute_sql("answer.sql")
print ("Correct answer: ", correct_answer)

correct_participants = []
wrong_participants = []

folder_prefix = "{}_sqls".format(sys.argv[1])
for filename in os.listdir(folder_prefix):
    participant_answer = execute_sql(os.path.join(folder_prefix, filename))
    # print (participant_answer)

    if correct_answer == participant_answer:
        correct_participants.append(filename)
    else:
        wrong_participants.append(filename)

with open(sys.argv[1] + ".final", 'w') as f:
    f.write("Correct answers:\n")
    for item in correct_participants:
        f.write("{}\n".format(item))

    f.write("\nIncorrect answers:\n")
    for item in wrong_participants:
        f.write("{}\n".format(item))
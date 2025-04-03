import csv
import sqlite3
import re

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):

    with open(file_path) as file:
        values=csv.reader(file)
        next(values)
        results=[]
        for i in values:
            if len(i)<2:
                continue
            first_name=i[0]
            last_name=i[1]
            if len(i)>2:
                last_name=i[2]
            first_name=re.sub(r"[^a-zA-Z]","",first_name)
            last_name=re.sub(r"[^a-zA-Z]","",last_name)
            if first_name and last_name:
                results.append((first_name,last_name))
        cursor.executemany('''INSERT INTO users(firstName,lastName) VALUES (?,?)''',results)
        conn.commit()
            

# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):

    with open(file_path,"r") as file:
        values=csv.reader(file)
        next(values)
        results=[]
        for i in values:
            if len(i)<5:
                continue
            if len(i)>5:
                i=i[:5]
            phno,start,end,direc,uid=i[0],i[1],i[2],i[3],i[4]
            phnolist=re.findall(r"\d{3}-\d{3}-\d{4}",phno)
            if not phnolist:
                continue
            phno=phnolist[0]
            try:
                start=int(float(start))
                end=int(float(end))
            except ValueError:
                continue
            if direc not in ["inbound","outbound"]:
                continue
            if not uid.isdigit():
                continue
            if phno and start and end and direc and uid:
                results.append((phno,start,end,direc,int(uid)))
        cursor.executemany('''INSERT INTO callLogs(phoneNumber,startTime,endTime,direction,userId) VALUES(?,?,?,?,?)''',results)
        conn.commit()


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):

    cursor.execute('''SELECT userId,startTime,endTime FROM callLogs''')
    values=cursor.fetchall()
    call_duration={}
    call_counts={}
    for user,start,end in values:
        if user is None or start is None or end is None:
            continue
        duration=end-start
        if user in call_duration:
            call_duration[user]+=duration
            call_counts[user]+=1
        else:
            call_duration[user]=duration
            call_counts[user]=1

    with open(csv_file_path,"w",newline="") as file:
        write=csv.writer(file)
        write.writerow(["userId","avgDuration","numCalls"])
        for u in sorted(call_duration.keys(),key=int):
            avg_d=round(call_duration[u]/call_counts[u],2)
            write.writerow([user,avg_d,call_counts[u]])
        conn.commit()



# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):

    cursor.execute('''SELECT * FROM callLogs ORDER BY userId,startTime''')
    values=cursor.fetchall()
    with open(csv_file_path,"w",newline="") as file:
        write=csv.writer(file)
        write.writerow(["callId","phoneNumber","startTime","endTime","direction","userId"])
        write.writerows(values)
    conn.commit()



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()

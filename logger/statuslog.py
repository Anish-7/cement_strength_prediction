import datetime

def log(path,msg):
    """
    path defines the path of file to be logged
    msg defines the logging status messge
    """
    time_now= datetime.datetime.now() # take timedate of now

    date = time_now.date() # filter only date
    time = time_now.strftime("%H:%M:%S") # filter only time accordng to the fomat

    log_file = open(path ,'a')
    log_msg = f'{date}\t{time}\t{msg}\n'
    log_file.write(log_msg)
    log_file.close()
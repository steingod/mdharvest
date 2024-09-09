"""
PURPOSE:
    Utility functions used by several scripts in the parent folder.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2024-09-09 

UPDATED:
    Øystein Godøy, METNO/FOU, 2024-09-09 
        Moved to module

NOTES:

"""

def initialise_logger(outputfile, name):
    # Check that logfile exists
    logdir = os.path.dirname(outputfile)
    if not os.path.exists(logdir):
        try:
            os.makedirs(logdir)
        except:
            raise IOError
    # Set up logging
    mylog = logging.getLogger(name)
    mylog.setLevel(logging.INFO)
    #logging.basicConfig(level=logging.INFO,
    #        format='%(asctime)s - %(levelname)s - %(message)s')
    myformat = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(myformat)
    mylog.addHandler(console_handler)
    file_handler = logging.handlers.TimedRotatingFileHandler(
            outputfile,
            when='w0',
            interval=1,
            backupCount=7)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(myformat)
    mylog.addHandler(file_handler)

    return(mylog)



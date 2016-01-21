######################################
# @Author : Mohit Jain               #
# @Email  : develop13mohit@gmail.com #
######################################

import os
import sys

def fetchFiles(path):
    if not os.path.isdir(path):
        print "[ERROR] Invalid path, does not exist... ", path
        print "Try again?\n Path : ",
        path = raw_input()
        fetchFiles(path)
    else:
        data_files = os.listdir(path)
        print "[INFO] Found %s database... Now loading." % len(data_files)
        return data_files
        
def main():
    """
    Initializer Function.
    """
    # Print Fancy Stuff
    # TO-DO : do this.

    # Initialze Database.
    print "Please enter path to the database files :",
    path = raw_input()
    databases = fetchFiles(path)

if __name__=='__main__':
    main()

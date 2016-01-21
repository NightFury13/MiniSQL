######################################
# MiniSQL - A mini database system   #
# @Author : Mohit Jain               #
# @Email  : develop13mohit@gmail.com #
######################################

# Library imports
import os
import sys
from termcolor import colored, cprint
import json
import csv
import sqlparse

def fetchFiles(path):
    """
    Check if path provided exists. If so, load the data files.
    """
    if not os.path.isdir(path):
        print colored("[ERROR]",'red'),"Invalid path, does not exist... ", path
        print "Try again?\n Path : ",
        new_path = raw_input()
        new_path, new_data = fetchFiles(new_path)
        return new_path, new_data
    else:
        data_files = os.listdir(path)
        print colored("[INFO]",'green')+" Found %s files... Now populating database." % len(data_files)
        return path, data_files

def groupGenerator(seq, delimitor):
    """
    Group the elements of a list based on a delimiter word.
    """
    group = []
    for ele in seq:
        if ele==delimitor:
            yield group
            group = []
        group.append(ele)
    yield group

def loadDatabases(path, data_files):
    """
    Check if the metadata file exists and load the table contents accordingly.
    """
    if "metadata.txt" not in data_files:
        print colored("[ERROR]",'red'),"Metadata file (metadata.txt) not found in provided database path!"
        return "error"
    else:
        try:
            with open(path+'/metadata.txt','r') as meta_file:
                meta_content = meta_file.read().splitlines()
                tables = filter(None, list(groupGenerator(meta_content, "<begin_table>")))
                meta_tables = {}
                for table in tables:
                    t_name = table[1]
                    meta_tables[t_name] = {}
                    for col in table[2:-1]:
                        meta_tables[t_name][col] = []
            print 
            print colored("> Database Schema : ","cyan")
            print colored(json.dumps(meta_tables,sort_keys=True, indent=4),'cyan')

            # Fill the data values
            for table_f in data_files:
                if table_f=='metadata.txt':
                    continue
                else:
                    with open(path+'/'+table_f,'r') as tab_f:
                        t_name = table_f.split('.')[0]
                        table_content = [row for row in csv.reader(tab_f, delimiter=',',skipinitialspace=True)]
                        header, contents = table_content[0], table_content[1:]
                        for row in contents:
                            for col in meta_tables[t_name]:
                                meta_tables[t_name][col].append(int(row[header.index(col)]))
            print colored("\n[INFO]",'green'),"Loading data values complete.\n"
            return meta_tables
        except:
            print colored("[ERROR]",'red'),"Metadata file maybe corrupt! Format mismatch."
            return "error"

def parseQuery(query):
    """
    Parse the query and return the tokens of query.
    """
    tokens = filter(None, [str(x).strip() for x in sqlparse.parse(query)[0].tokens])

    select = [x.strip() for x in tokens[1].split(',')]
    tables = [x.strip() for x in tokens[3].split(',')]
    conditions = [x.strip().split(';')[0] for x in '='.join([x.strip() for x in tokens[-1].split('=')]).split(' ')[1:]]
    print colored("Select",'green'), select
    print colored("Tables",'green'), tables
    print colored("Conditions",'green'), conditions
    return select, tables, conditions

def startEngine():
    """
    Main controller function for taking query inputs and displaying respective outputs.
    """
    print colored("MiniSQL>",'cyan'),
    query = raw_input()
    while query!='q':
        select, tables, conditions = parseQuery(query)
 
        # Take next query.
        print colored("MiniSQL>",'cyan'),
        query = raw_input()

    print colored("Thanks for using MiniSQL. Exiting Now...",'yellow')

def main():
    """
    Initializer Function.
    """
    # Print Fancy Stuff
    # TO-DO : do this.

    # Initialze Database.
    print "Please enter path to the database files :",
    path = raw_input()
    path, data_files = fetchFiles(path)
    database = loadDatabases(path, data_files)
    if database=="error":
        print 
        print colored('Review the errors and try again','yellow')
        return 

    print colored("           Welcome to the MiniSQL Engine\n","yellow")
    print colored("~ Enter your query on the prompt",'yellow')
    print colored("~ q : Quit\n",'yellow')
    # Start the query engine.
    startEngine()


if __name__=='__main__':
    main()

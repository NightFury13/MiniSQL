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
import copy
from terminaltables import AsciiTable

# Global Variables
database_path = ''

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
                        try:
                            header, contents = table_content[0], table_content[1:]
                        except:
                            print colored("[INFO]",'red'),table_f, "database is empty."
                            header = []
                            contents = []
                        for row in contents:
                            for col in meta_tables[t_name]:
                                try:
                                    meta_tables[t_name][col].append(int(row[header.index(col)]))
                                except:
                                    print colored("[ERROR]",'red'),"loading value %s failed, must be integer. Storing NULL instead" % str(row[header.index(col)])
                                    meta_tables[t_name][col].append("NULL")

            print colored("\n[INFO]",'green'),"Loading data values complete.\n"
            return meta_tables
        except:
            print colored("[ERROR]",'red'),"Metadata file maybe corrupt! Format mismatch."
            return "error"

def parseQuery(query):
    """
    Parse the query and return the tokens of query.
    """
    select = []
    tables = []
    conditions = []
    
    tokens = filter(None, [str(x).strip() for x in sqlparse.parse(query)[0].tokens])
    if ";" in tokens:
        tokens.remove(";")
    
    if ('create' in tokens) or ('CREATE' in tokens):
        tokens = tokens[-1]
        tokens = tokens.split('(')
        table_name = tokens[0].strip()
        table_cols = [x.strip() for x in tokens[1].split(')')[0].split(',')]
        select.append(table_name)
        tables+=table_cols
        conditions.append('create_table')
        print colored("Table",'green'), select
        print colored("Columns",'green'), tables
        print colored("To Do",'green'), conditions
        return select, tables, conditions

    if ('insert' in tokens) or ('INSERT' in tokens):
        table = tokens[-2]
        values = [x.strip() for x in tokens[-1].split(',')]
        print colored("Table",'green'), table
        print colored("Values",'green'), values
        return ['insert'], table, values

    if ('delete' in tokens) or ('DELETE' in tokens):
        table = tokens[2]
        condition = tokens[-1].split('where')[1].strip().split(';')[0].strip()
        print colored("Table",'green'), table
        print colored("Condition",'green'), condition
        return ['delete'], [table], condition

    if ('truncate' in tokens) or ('TRUNCATE' in tokens):
        table = tokens[2]
        print colored("Table",'green'), table
        print colored("To Do",'green'),'Delete records'
        return ['truncate'], [table], []

    if ('drop' in tokens) or ('DROP' in tokens):
        table = tokens[-1]
        print colored("Table",'green'), table
        print colored("To Do",'green'), 'Drop Table'
        return ['drop'], [table], []

    if len(tokens) < 4:
        print colored("[ERROR]",'red'),"Invalid query! FROM & SELECT parameters are mandatory"
        return "error"
    elif len(tokens) > 4:
        if ('and' in tokens[-1]) or ('AND' in tokens[-1]):
            try:
                conds = tokens[-1].split('and')
            except:
                conds = tokens[-1].split('AND')
        else:
            conds = [tokens[-1]]
        for cond in conds:
            delim = ''
            if '=' in cond:
                delim = '='
            elif '<' in cond:
                delim = '<'
            elif '>' in cond:
                delim = '>'
            else:
                print colored("[ERROR]",'red'),"Invalid operator! Only =,>,< are supported."
                return "error"
            conditions+=[x.strip().split(';')[0] for x in [delim.join([x.strip() for x in cond.split(delim)]).split(' ')[-1]]]
        
    select = [x.strip() for x in tokens[1].split(',')]
    tables = [x.strip() for x in tokens[3].split(',')]

    print colored("Select",'green'), select
    print colored("Tables",'green'), tables
    print colored("Conditions",'green'), conditions
    
    return select, tables, conditions

def computeQuery(select, tables, conditions, database):
    """
    Calculate the query output.
    """
    global database_path
    output = copy.deepcopy(database)
    if 'create_table' in conditions:
        table_name = select[0]
        if table_name+'.csv' not in os.listdir(database_path):
            with open(database_path+'/metadata.txt','a') as f:
                f.write('<begin_table>\n%s\n'%table_name)
                for col in tables:
                    f.write('%s\n'%col)
                f.write('<end_table>\n')
            with open(database_path+'/'+table_name+'.csv','w') as f:
                for col in tables[:-1]:
                    f.write(col+',')
                f.write(tables[-1])
            print colored("[DONE]",'yellow'),"New Table %s created with columns : %s" % (table_name,','.join(tables))
            return "table_created"
        else:
            print colored("[ERROR]",'red'),"Table with name %s already exists!" % table_name
            return "error"

    if 'insert' in select:
        table_name = tables
        values = conditions
        if table_name+'.csv' in os.listdir(database_path):
            with open(database_path+'/'+table_name+'.csv','a') as f:
                for val in values[:-1]:
                    f.write(val+',')
                f.write(values[-1]+'\n')
            return "data_inserted"
        else:
            print colored("[ERROR]",'red'),"Unsupported value format!"
            return "error"

    if 'drop' in select:
        table = tables[0]
        if output[table][output[table].keys()[0]]:
            try:
                os.remove(database_path+'/'+table+'.csv')
                with open(database_path+'/metadata.txt','w') as f:
                    for tab in output:
                        if not tab==table:
                            f.write('<begin_table>\n')
                            f.write(tab+'\n')
                            for col in sorted(output[tab].keys()):
                                f.write(col+'\n')
                            f.write('<end_table>\n')
                return "table_dropped"
            except:
                print colored("[ERROR]",'red'),"Failed to remove table!"
                return "error"
        else:
            print colored("[ERROR]",'red'),'Table not empty! Try truncating first...'
            return "error"

    out_tables = output.keys()
    for table in out_tables:
        if table not in tables:
            output.pop(table, None)
    
    if 'delete' in select:
        table_name = tables[0]
        delim = ''
        if '=' in conditions:
            delim = '='
        elif '>' in conditions:
            delim = '>'
        elif '<' in conditions:
            delim = '<'
        else:
            print colored("[ERROR]",'red'),"Invalid operator! Only =,>,< are supported."
            return "error"
        col, val = [x.strip() for x in conditions.split(delim)]
        print col, val
        try:
            del_idx = output[table_name][col].index(int(val))
            for col in output[table_name]:
                del_ele = output[table_name][col].pop(del_idx)
            with open(database_path+'/'+table_name+'.csv','w') as f:
                cols = sorted(output[table_name].keys())
                for col in cols[:-1]:
                    f.write(col+',')
                f.write(cols[-1]+'\n')
                for i in range(len(output[table_name][cols[0]])):
                    for col in cols[:-1]:
                        f.write(str(output[table_name][col][i])+',')
                    f.write(str(output[table_name][cols[-1]][i])+'\n')
            return "data_deleted"
        except:
            print colored("[ERROR]",'red'),"No matching data-entry found!"
            return "error"

    if 'truncate' in select:
        table = tables[0]
        try:
            with open(database_path+'/'+table+'.csv','w') as f:
                cols = sorted(output[table].keys())
                for col in cols[:-1]:
                    f.write(col+',')
                f.write(cols[-1]+'\n')
            return "data_truncated"
        except:
            print colored("[ERROR]",'red'), "No matching table found!"
            return "error"


    if ('or' not in conditions) and ('OR' not in conditions):
        for cond in conditions:
            if cond=='AND' or cond=='and':
                continue
            delim = ''
            if '=' in cond:
                field, value = cond.split('=')
                delim = '='
            elif '>' in cond:
                field, value = cond.split('>')
                delim = '>'
            elif '<' in cond:
                field, value = cond.split('<')
                delim = '<'
            else:
                print colored("[ERROR]",'red'),"Invalid operator! Only =,>,< are supported."
                return "error"

            try:
                value = int(value)
            except:
                print colored("[ERROR]",'red'),'Values can only be integers! Not %s.' % str(value) 
                return "error"
            for table in tables:
                if field in output[table]:
                    del_field = []
                    for i in range(len(output[table][field])):
                        if delim=='=':
                            if output[table][field][i] != value:
                                del_field.append(i)
                        elif delim=='>':
                            if output[table][field][i] <= value:
                                del_field.append(i)
                        elif delim=='<':
                            if output[table][field][i] >= value:
                                del_field.append(i)
                    for i in sorted(del_field, reverse=True):
                        for field_del in output[table]:
                            output[table][field_del].remove(output[table][field_del][i])
    # OR type of queries                        
    else:
        delim1 = ''
        delim2 = ''
        cond1 = conditions[0]
        cond2 = conditions[-1]
        if '=' in cond1:
            field1, value1 = cond1.split('=')
            delim1 = '='
        elif '>' in cond1:
            field1, value1 = cond1.split('>')
            delim1 = '>'
        elif '<' in cond1:
            field1, value1 = cond1.split('<')
            delim1 = '<'
        else:
            print colored("[ERROR]",'red'),"Invalid operator! Only =,>,< are supported."
            return "error"

        if '=' in cond2:
            field2, value2 = cond2.split('=')
            delim2 = '='
        elif '>' in cond2:
            field2, value2 = cond2.split('>')
            delim2 = '>'
        elif '<' in cond2:
            field2, value2 = cond2.split('<')
            delim2 = '<'
        else:
            print colored("[ERROR]",'red'),"Invalid operator! Only =,>,< are supported."
            return "error"
        try:
            value1 = int(value1)
            value2 = int(value2)
        except:
            print colored("[ERROR]",'red'),'Values can only be integers! Not %s or %s.' % (str(value1), str(value2)) 
           
        for table in tables:
            if (field1 in output[table]) or (field2 in output[table]): 
                if (field1 in output[table]) and (field2 not in output[table]):
                    field = field1
                    value = value1
                    delim = delim1
                else:
                    field = field2
                    value = value2
                    delim = delim2
                del_field = []
                for i in range(len(output[table][field])):
                    if delim=='=':
                        if output[table][field][i] != value:
                            del_field.append(i)
                    elif delim=='>':
                        if output[table][field][i] <= value:
                            del_field.append(i)
                    elif delim=='<':
                        if output[table][field][i] >= value:
                            del_field.append(i)
                for i in sorted(del_field, reverse=True):
                    for field_del in output[table]:
                        output[table][field_del].remove(output[table][field_del][i])
            
    if len(select)==1:
        ele = select[0]
        if not ele=='*':            
            field = ele.split('(')[-1].split(')')[0]
            for table in tables:
                if field in output[table]:
                    if 'max' in ele:
                        output[table][field] = [max(output[table][field])]
                    elif 'min' in ele:
                        output[table][field] = [min(output[table][field])]
                    elif 'avg' in ele:
                        output[table][field] = [float(sum(output[table][field]))/len(output[table][field])]
                    elif 'sum' in ele:
                        output[table][field] = [sum(output[table][field])]
                    elif 'count' in ele:
                        output[table][field] = [len(output[table][field])]
                    elif 'distinct' in ele:
                        output[table][field] = [list(set(output[table][field]))]
                    del_fields = output[table].keys()
                    for del_f in del_fields:
                        if del_f != field:
                            output[table].pop(del_f, None)
                else:
                    output.pop(table, None)
    else:
        for table in tables:
            del_fields = output[table].keys()
            for del_f in del_fields:
                if del_f not in select:
                    output[table].pop(del_f, None)
            
    return output

def startEngine(database):
    """
    Main controller function for taking query inputs and displaying respective outputs.
    """
    print colored("MiniSQL>",'cyan'),
    query = raw_input()
    while query!='q':
        try:
            if query.split(';')[0].lower()=='rebase data':
                global database_path
                path, data_files = fetchFiles(database_path)
                database = loadDatabases(path, data_files)
                if database=="error":
                    print colored("[ERROR]",'red'), "Database corrupted! Manually edit data to verify."
                    query='q'
                    continue
                print colored("MiniSQL>",'cyan'),
                query = raw_input()
                continue

            select, tables, conditions = parseQuery(query)
            output = computeQuery(select, tables, conditions, database) 
            if output == 'error':
                print colored("Retry with supported operations?",'yellow')
            elif output in ['table_created','data_inserted','data_deleted','data_truncated','table_dropped']:
                query = 'rebase data'
                continue
            # Print output in pretty table format.
            else:
                table_data = []
                for table in output:
                    table_data.append([table+'.'+x for x in sorted(output[table].keys())])
                    #table_data[0]+=[table+'.'+x for x in sorted(output[table].keys())]
                    for i in range(len(output[table][output[table].keys()[0]])):
                        row = []
                        for col in sorted(output[table].keys()):
                            row.append(str(output[table][col][i]))
                        table_data.append(row)
                    table_data.append([])
                table = AsciiTable(table_data,' '.join(output.keys()))
                print ""
                print table.table
        except:
            print colored("Retry with supported operators?",'yellow')

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
    global database_path
    database_path = path
    path, data_files = fetchFiles(path)
    database = loadDatabases(path, data_files)
    if database=="error":
        print 
        print colored('Review the errors and try again','yellow')
        return 

    print colored("           Welcome to the MiniSQL Engine\n","yellow")
    print colored("~ Enter your query on the prompt",'yellow')
    print colored("~ rebase data : Use after every database addition/deletion",'yellow')
    print colored("~ q : Quit\n",'yellow')

    # Start the query engine.
    startEngine(database)


if __name__=='__main__':
    main()

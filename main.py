#!/usr/bin/python
import sqlparse
from collections import defaultdict
import os
import re
import sys
from prettytable import PrettyTable
import time
from ColorizePython import pycolors
from copy import deepcopy

DATASET_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/")

schema = {}
dataset = defaultdict(list)
start_time = 0

AGGREGATE_FUNCTIONS = ("distinct", "avg", "max", "min", "sum")

class MyException(Exception):
    pass


def get_aggregate_function(col, tables):
    """ Returns the extracted function from the column with the column name
        Returns: column_name, function_name
    """
    count = []
    function = None
    for func in AGGREGATE_FUNCTIONS:
        reg = re.compile(r'(%s)\(([a-zA-Z]{1,})\)' %(func))
        col2 = re.sub(reg, r'\2', col)
        function = func
        if col != col2:
            break
    if col == col2:
        return col, None

    return get_tables_generic_name(col2, tables), function


def handle_query(query):
    """ THE query must be formatted
    """
    try:
        stage = None
        columns = []
        tables = []
        conditionals = []
        aggregate_functions_map = []    # of the format [(col1, distinct), (col2, sum)]


        # Parse query into logic ends
        for line in query.split("\n"):
            line = line.split()
            if line[0] in ("SELECT", "FROM", "WHERE") and len(line) >= 2:
                stage = line[0]

            if stage == "SELECT":
                columns.append(clean(line[-1].strip(",")))
            elif stage == "FROM":
                if line[0] == "FROM":
                    tables.append(clean(" ".join(line[1:]).strip(",")))
                else:
                    tables.append(clean(" ".join(line).strip(",")))
            elif stage == "WHERE":
                if line[0] == "WHERE":
                    conditionals.append("".join(line[1:]))
                else:
                    conditionals.append(" ".join(line))
            else:
                raise MyException("InvalidQueryLanguage")

        # split conditionals and re-arrange branckets ["(", ")")]
        conditionals = re.sub(re.compile(r'(\(|\))'), r' \1 ', " ".join(conditionals)).split()
        reg = re.compile(r'(=|!=|>|<|<=|>=)')
        conditionals = [tuple(re.sub(reg, r' \1 ', i).split()) if i not in ("AND", "OR", "(", ")") else i for i in conditionals]


        # Verify table names
        new_tables = []
        for tab in tables:
            if tab not in schema.keys():
                # Check and Handle aliases
                tab = tab.split()
                if len(tab) == 3 and (tab[0] in schema.keys()) and tab[1].lower() == "as":
                    create_table_alias(tab[0], tab[2])
                    tab = tab[2]
                else:
                    raise MyException('TableNotPresentInDatabase')
            new_tables.append(tab)
        tables = new_tables

        # Check conflicting column names
        new_columns = []
        for col in columns:
            if col == "*":
                pass
            elif "." not in col:
                count = []
                for table in tables:
                    if ".".join([table, col]) in schema[table]:
                        count.append(table)
                if len(count) > 1:
                    raise MyException('ColumnNameConflict')
                elif len(count) == 0:
                    col, func = get_aggregate_function(col, tables)
                    if not func:
                        raise MyException('ColumnNotPresentInTable')
                    aggregate_functions_map.append((col, func))
                else:
                    col = count[0] + "." + col
            else:
                if col not in schema[table]:
                    raise MyException('ColumnNotPresentInTable')
            new_columns.append(col)
        columns = new_columns

        # Create master table schema, join 'em all!
        new_schema = ()
        for tab in tables:
            new_schema += schema[tab]

        # Create master dataset, merge 'em all!
        new_dataset = [{}]
        for table in tables:
            dataset2 = []
            for x in dataset[table]:
                for y in new_dataset:
                    z = {}
                    z.update(x)
                    z.update(y)
                    dataset2.append(z)
            new_dataset = dataset2

        # Apply the conditions
        final_dataset = []
        for row in new_dataset:
            new_cond = []
            for condition in conditionals:
                if condition in ("AND", "OR", ")", "("):
                    new_cond.append(condition.lower())
                else:
                    [tabcolumn, operator, check] = condition
                    tabcolumn = get_tables_generic_name(tabcolumn, tables)
                    try:
                        check = int(clean(check))
                    except ValueError:
                        tab2 = get_tables_generic_name(check, tables)
                        check = row[tab2]

                    if (operator == "=" and row[tabcolumn] == check) or \
                       (operator == ">" and row[tabcolumn] > check) or \
                       (operator == "<" and row[tabcolumn] < check) or \
                       (operator == "!=" and row[tabcolumn] != check) or \
                       (operator == "<=" and row[tabcolumn] <= check) or \
                       (operator == ">=" and row[tabcolumn] >= check):
                        new_cond.append("True")
                    else:
                        if operator not in ("=", ">", "<", "!=", "<=", ">="):
                            raise MyException('InvalidOperator')
                        new_cond.append("False")

            # Just EVAL the final string
            if eval(" ".join(new_cond) if len(new_cond) > 0 else "True"):
                final_dataset.append(row)

    except Exception as e:
        # Raise exception for the next parent catch block.
        # raise e
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print "ERROR:", str(exc_type), "on", exc_tb.tb_lineno

    return new_schema, columns, final_dataset, aggregate_functions_map
    # Look for conditionals that can reduce computation !!
    # for condition in conditionals:
    #     if len(condition) <= 1:
    #         continue
    #     try:
    #         integer = int(condition[2])
    #
    #     except ValueError:
    #         pass

    # Re-parse columns, i.e. *, conflicts etc


def process_aggregate_function(agg_col, agg_func, final_dataset, new_schema):
    """
        Processes the aggregate function and modifies the final_dataset as per the need.
        Returns: new_schema, processed_final_dataset
    """
    new_col_name = agg_func + "(" + agg_col + ")"
    if agg_func == "sum":
        s = sum(row[agg_col] for row in final_dataset)
        final_dataset = [{new_col_name: s}]
        return [new_col_name], final_dataset
    elif agg_func == "avg":
        s = 1.0 * sum(row[agg_col] for row in final_dataset) / len(final_dataset)
        final_dataset = [{new_col_name: s}]
        return [new_col_name], final_dataset
    elif agg_func == "max":
        s = max(row[agg_col] for row in final_dataset)
        final_dataset = [{new_col_name: s}]
        return [new_col_name], final_dataset
    elif agg_func == "min":
        s = min(row[agg_col] for row in final_dataset)
        final_dataset = [{new_col_name: s}]
        return [new_col_name], final_dataset
    elif agg_func == "distinct":
        distinct = []
        new_data = []
        for row in final_dataset:
            if row[agg_col] in distinct:
                continue
            distinct.append(row[agg_col])
            row[new_col_name] = row.pop(agg_col)
            new_data.append(row)
        return [new_col_name if col == agg_col else col for col in new_schema], new_data
    else:
        raise MyException('CannotProcessTheAggregateFunction')


def output(new_schema, aggregate_functions_map, columns, final_dataset):
    """ Create a pretty table output similar to SQL """
    field_names = []
    if "*" in columns:
        field_names = list(new_schema)
    else:
        if len(aggregate_functions_map) >= 1:
            if len(aggregate_functions_map) > 1:
                print aggregate_functions_map
                raise MyException('MultipleAggregateFunctionNotAllowed')
            (agg_col, agg_func) = aggregate_functions_map[0]
            aggregate_functions_dict = dict(aggregate_functions_map)
            field_names, final_dataset = process_aggregate_function(agg_col, agg_func, final_dataset, new_schema)
        else:
            field_names = [col for col in columns if col != "*"]

    x = PrettyTable(field_names)
    for row in final_dataset:
        x.add_row([row[field] for field in x.field_names])
    print x
    return final_dataset


def output_summary(received_dataset):
    print pycolors.OKGREEN+pycolors.BOLD, len(received_dataset), "records generated in", time.time() - start_time, "seconds", pycolors.ENDC


def create_table_alias(table_name, alias_name):
    """ Create a table alias by copying the dataset and the table schema """
    global dataset, schema
    if alias_name in schema.keys():
        raise MyException('CannotCreateAliasWithAlreadyExistingTableName')

    # Copy schema
    schema[alias_name] = tuple([".".join([alias_name, col.split(".")[1]]) for col in schema[table_name]])

    # Copy Dataset
    dataset[alias_name] = deepcopy(dataset[table_name])
    for row in dataset[alias_name]:
        for (key, val) in row.items():
            row[".".join([alias_name, key.split(".")[1]])] = deepcopy(row[key])
            del row[key]


def get_tables_generic_name(column_name, tables):
    """
    Converts the column names from "col" to "table.col"
    """
    if "." in column_name:   # No need to change as the name is already generic
        return column_name
    found = []
    for table in tables:
        if ".".join([table, column_name]) in schema[table]:
            found.append(table)
    if len(found) == 0:
        raise MyException('ColumnNotPresentInAnyTable')
    elif len(found) > 1:
        raise MyException('ColumnNameConflict')
    return ".".join([found[0], column_name])


def create_tables(filename):
    with open(DATASET_PATH + filename, "r") as f:
        table_found = -1
        for row in f.readlines():
            row = clean(row)
            if row == "<begin_table>":
                table_found = 0
            elif row == "<end_table>":
                table_found = -1
            else:
                if table_found == 0:
                    schema[row] = ()
                    table_found = row
                else:
                    if row[0] >= 48 and row[0] <= 57:
                        raise MyException('InvalidColumnName')
                    elif "." in row:
                        raise MyException('InvalidColumnName')
                    schema[table_found] += (table_found + "." + row,)


def load_data(filename=None):
    """ Load the data from the particular file.
        If the filename is not given then all the tables in the "data" folder are loaded.
    """
    # Load all the files if no file is given
    files = []
    if not filename:
        for filename in os.listdir(DATASET_PATH):
            filename = filename.split(".")
            if len(filename) != 2:
                continue
            if filename[1] == "csv" and filename[0] in schema.keys():
                files.append(".".join(filename))
    else:
        files.append(filename)

    # Load data from all the files
    for filename in files:
        print "Loading data from: %s" %(filename)
        filename = filename.split(".")
        if filename[1] != "csv":
            raise MyException('NotATableFile')
        tablename = filename[0]
        with open(DATASET_PATH + ".".join(filename), "r") as f:
            for row in f.readlines():
                if len(row) < 0:
                    continue
                split_row = row.split(",")
                if len(split_row) != len(schema[tablename]):
                    raise MyException('NoOfColumnsDifferentInInputFile')
                dataset[tablename].append(dict((column, int(clean(value)) if len(clean(value)) > 0 else 0) for i, (column, value) in enumerate(zip(schema[tablename], split_row))))


def clean(s):
    s = s.strip()
    while len(s) > 1 and (s[0] == '\"' or s[0] == '\'') and s[0] == s[-1]:
        s = s[1:-1]
    return s

if __name__ == "__main__":
    create_tables("metadata.txt")

    # load_data("table1.csv")
    # load_data("table2.csv")
    load_data()

    while True:
        try:
            command = raw_input(">> ").strip()
            start_time = time.time()
            if command.lower() == "exit" or command.lower() == "exit;":
                break
            while command.strip()[-1] != ";":
                command += " " + raw_input("... ")
            command = command[:-1]
            command = sqlparse.format(command, reindent=True, keyword_case='upper')

            new_schema, columns, final_dataset, aggregate_functions_map = handle_query(command)
            final_dataset = output(new_schema, aggregate_functions_map, columns, final_dataset)
            output_summary(final_dataset)
        except EOFError:
            print ""
        except KeyboardInterrupt:
            print ""
            break
        except MyException as e:
            print pycolors.BOLD + pycolors.FAIL + pycolors.UNDERLINE + "ERROR" + pycolors.ENDC + ": " + pycolors.BOLD + e.args[0] + pycolors.ENDC

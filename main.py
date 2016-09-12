#!/usr/bin/python
import sqlparse
from collections import defaultdict
import os
import re
# from freezer import freeze
import sys
from prettytable import PrettyTable
import time
from ColorizePython import pycolors

DATASET_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/")

schema = {}
dataset = defaultdict(list)
start_time = 0


class MyException(Exception):
    pass


def handle_query(query):
    """ THE query must be formatted
    """
    try:
        stage = None
        columns = []
        tables = []
        conditionals = []

        # Parse query into logic ends
        for line in query.split("\n"):
            line = line.split()
            if line[0] in ("SELECT", "FROM", "WHERE") and len(line) >= 2:
                stage = line[0]

            if stage == "SELECT":
                columns.append(clean(line[-1].strip(",")))
            elif stage == "FROM":
                tables.append(clean(line[-1].strip(",")))
            elif stage == "WHERE":
                if line[0] != "WHERE":
                    conditionals.append(line[0])
                conditionals.append(" ".join(line[1:]))
            else:
                raise MyException("InvalidQueryLanguage")

        # split conditionals and re-arrange branckets ["(", ")")]
        conditionals = re.sub(re.compile(r'(\(|\))'), r' \1 ', " ".join(conditionals)).split()
        reg = re.compile(r'(=|!=|>|<|<=|>=)')
        conditionals = [tuple(re.sub(reg, r' \1 ', i).split()) if i not in ("AND", "OR", "(", ")") else i for i in conditionals]


        # Verify table names
        for tab in tables:
            if tab not in schema.keys():
                raise MyException('TableNotPresentInDatabase')

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
                    raise MyException('ColumnNotPresentInTable')
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
        raise e

        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print "ERROR:", str(exc_type), "on", exc_tb.tb_lineno

    return new_schema, columns, final_dataset
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


def output(new_schema, columns, final_dataset):
    # import pdb; pdb.set_trace()
    x = PrettyTable()
    x.field_names = []
    if "*" in columns:
        x.field_names += list(new_schema)
    else:
        for col in columns:
            if col == "*":
                continue
            x.field_names += [col]

    for row in final_dataset:
        x.add_row([row[field] for field in x.field_names])
    print x

def output_summary(final_dataset):
    print pycolors.OKGREEN+pycolors.BOLD, len(final_dataset), "records generated in", time.time() - start_time, "seconds", pycolors.ENDC


def get_tables_generic_name(table_name, tables):
    """
    Converts the table names from "col" to "table.col"
    """
    if "." in table_name:   # No need to change as the name is already generic
        return table_name
    found = []
    for table in tables:
        if ".".join([table, table_name]) in schema[table]:
            found.append(table)
    if len(found) == 0:
        raise MyException('ColumnNotPresentInAnyTable')
    elif len(found) > 1:
        raise MyException('ColumnNameConflict')
    return ".".join([found[0], table_name])


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


def load_data(filename):
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
    # schema = freeze(schema)

    load_data("table1.csv")
    load_data("table2.csv")
    # dataset = freeze(dataset)

    while True:
        try:
            command = raw_input(">> ")
            start_time = time.time()
            if command.lower() == "exit" or command.lower() == "exit;":
                break
            while command.strip()[-1] != ";":
                command += " " + raw_input("... ")
            command = command[:-1]
            command = sqlparse.format(command, reindent=True, keyword_case='upper')

            new_schema, columns, final_dataset = handle_query(command)
            output(new_schema, columns, final_dataset)
            output_summary(final_dataset)
        except EOFError:
            print ""
        except KeyboardInterrupt:
            print ""
            break
        except MyException as e:
            print pycolors.BOLD + pycolors.FAIL + pycolors.UNDERLINE + "ERROR" + pycolors.ENDC + ": " + pycolors.BOLD + e.args[0] + pycolors.ENDC

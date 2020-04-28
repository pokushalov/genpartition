import sys
from loguru import logger
from OracleHelper import OracleHelper
import sqls
import config
import argparse
###################################################
__author__ = "Alex Pokushalov"
__version__ = "1.0.1"
__maintainer__ = "Alex Pokushalov"
__email__ = "alex@$LASTNAME.com"

########################################################################################################################
def parseArgs():
    parser = argparse.ArgumentParser(description='This is Partition Generator',
                                     epilog='--\nHave a great day from partition Merlin')
    parser.add_argument('--username', help='Username for DB connection', required=True)
    parser.add_argument('--password', help='Password for DB connection', required=True)
    parser.add_argument('--owner', help='Table owner for parittioning process', required=True)
    parser.add_argument('--table', help='Table name for parittioning process', required=True)
    parsed_args = parser.parse_args()
    return parsed_args
########################################################################################################################

def main() -> int:
    # init values
    try:
        logger.debug("Getting arguments")
        args = parseArgs()
        conn1_info = config.database_connection

        _table_owner = args.owner.upper()
        _table_name = args.table.upper()

        # for all constraints
        constraints = {}
        usage = {}
        # for indexes that used to support constraints
        auto_created_indexes = []
        # for all indexes, both created or ones that supporting constraints (like PK, UK)
        all_indexes = []
        # for all triggers for table
        all_triggers = []
        # action_plan - is for storing advices for partitioning
        # 2 dimensional array, [0] - is status ("ERROR" or "ADVICE"), [1] - Message
        action_plan = []

        logger.add("logs/magic.log", rotation="1 days", backtrace=True)
        logger.debug("Application started")
        conn1 = OracleHelper(conn1_info, logger, debug_sql=True)
        conn1._connect(username=args.username, password=args.password)


        # generate tuples
        src_table = (_table_owner, _table_name)
        src_table_ddl = ('TABLE', _table_owner, _table_name)
        # pre-check step
        res = conn1.runSelect(sqls.sql['table_precheck1'], src_table)
        if res[0][0] == 0:
            logger.critical(f"Table {_table_owner}.{_table_name} do not exists.")
            exit(-1)
        res = conn1.runSelect(sqls.sql['table_precheck2'], src_table)
        if res[0][0] == 1:
            logger.critical(f"Table {_table_owner}.{_table_name} already partitioned.")
            exit(-1)
        # don't forget to rename table:
        # for constraints and indexes we will create dictionaries so we can store
        # them to get DDL for later usage
        # get all constraints for this table
        res = conn1.runSelect(sqls.sql['all_constraints'], src_table)
        for item in res:
            if item[18] is not None:
                idx_str = f", using index {item[17]}.{item[18]}"
                auto_created_indexes.append(item[18])
            else:
                idx_str = ""
            logger.debug(f"Constraint: {item[0]}.{item[1]} with type {item[2]}{idx_str}")
            logger.debug(f"INFO :{item}")
            constraints[item[1]] = item
        # for indexes we will need to check all indexes in order to eliminate indexes that
        # already done via constraints
        indexes = {}
        res = conn1.runSelect(sqls.sql['all_indexes'], src_table)
        for item in res:
            if item[1] in auto_created_indexes:
                all_indexes.append([item[0], item[1], item])
                logger.warning(f"Index P{item[1]} already in list via constraint part, skipping")
            else:
                indexes[item[1]] = item
                all_indexes.append([item[0], item[1], item])

        res = conn1.runSelect(sqls.sql['all_triggers'], src_table)
        for item in res:
            all_triggers.append(item)
            logger.debug(item)

        #
        # SIZES
        # lets get needed sizes in TBS
        # for table
        res = conn1.runSelect(sqls.sql['table_size'], src_table)
        for item in res:
            logger.debug(f"Table size: {item[0]:,}")
        # for all indexes
        for value in all_indexes:
            logger.debug(f"Getting size for the index {value[0]}.{value[1]}")
            idx_tuple = (value[0], value[1])
            res = conn1.runSelect(sqls.sql['table_size'], idx_tuple)
            # we need to check index size return value, if index is empty - we need to handle this situation
            if len(res) > 0:
                tbs_name = value[2][9]
                space_used = res[0][0]
                logger.info(f"And size is {space_used:,} in TBS {tbs_name}")
                usage[tbs_name] = usage.get(tbs_name, 0) + space_used
            else:
                logger.info(f"Looks like index {value[0]}.{value[1]} is empty, 0 size")

        for (key, value) in usage.items():
            tbs_name = (key,)
            res = conn1.runSelect(sqls.sql['tablespace_free_space'], tbs_name)
            free_space_in_tbs = res[0][1]
            logger.info(f"Total usage by objects in [{key}]: {value:,}, and free space is {free_space_in_tbs:,}")
            free_space_proficit = free_space_in_tbs - value
            if free_space_proficit > 0:
                logger.success("We have enough space in TBS")
            else:
                logger.warning(f"We don't have enough space in TBS, need to add {abs(free_space_proficit):,}")
                action_plan.append(["ERROR",
                                    f"Please consider adding {abs(free_space_proficit):,} bytes to {key} tablespace [ {abs(free_space_proficit)} ]"])
        # Rename table to different name so we can create new  one:
        action_plan.append(['ADVISE', "-- Rename table "])
        action_plan.append(['ADVISE', f"alter table {_table_owner}.{_table_name} rename to {_table_name}_old;"])
        action_plan.append(['ADVISE', "-- Drop constraints "])
        # check for constraint here - should be only name
        for item in constraints.keys():
            action_plan.append(["ADVISE", f"alter table {_table_owner}.{_table_name}_old drop constraint {item};"])
        action_plan.append(['ADVISE', "-- Drop current indexes "])
        for item in all_indexes:
            action_plan.append(["ADVISE", f"drop index {item[0]}.{item[1]};"])

        # get ALL DDL's for table
        # let's prepare session before getting DDLs

        conn1.runPLSQL(sqls.sql['prepare_extract_ddl'])
        action_plan.append(['INFO', "-- Script generated by magic script, genpartitioner project"])
        action_plan.append(['ADVISE', "-- Generated table DDL "])
        action_plan.append(['ADVISE', "-- PLEASE ADD PARTITION PART INTO THIS SCRIPT "])
        res = conn1.runSelect(sqls.sql['object_ddl'], src_table_ddl)
        # for table DDL we will remove SEGMENT CREATION DEFERRED for now
        for item in res:
            logger.info(item[0])
            action_plan.append(['ADVISE', str(item[0]).replace("SEGMENT CREATION DEFERRED", "")])
        action_plan.append(['ADVISE', "-- Generated INDEX DDLs "])
        for item in all_indexes:
            tpl_index = ('INDEX', item[0], item[1])
            res = conn1.runSelect(sqls.sql['object_ddl'], tpl_index)
            res[0][0] = str(res[0][0]).replace(";", "local;")
            logger.info(res[0][0])
            action_plan.append(['ADVISE', res[0][0]])
        action_plan.append(['ADVISE', "-- Generated constraint DDLs "])
        for (key, value) in constraints.items():
            tpl_constraint = ('CONSTRAINT', value[0], value[1])
            res = conn1.runSelect(sqls.sql['object_ddl'], tpl_constraint)
            res[0][0] = str(res[0][0]).replace("USING INDEX", "USING INDEX  ")
            logger.info(res[0][0])
            action_plan.append(['ADVISE', res[0][0]])

        action_plan.append(['ADVISE', "-- Triggers "])
        for item in all_triggers:
            tpl_trigger = ('TRIGGER', item[0], item[1])
            res = conn1.runSelect(sqls.sql['object_ddl'], tpl_trigger)
            action_plan.append(['ADVISE', res[0][0]])
        action_plan.append(['ADVISE', "-- Grants "])
        # get grants on this table
        res = conn1.runSelect(sqls.sql['get_all_grants'], src_table)
        for item in res:
            logger.debug(f"Extracted grant: {item[0]}")
            action_plan.append(['ADVISE', item[0]])

        # adding last step for advice, fill newly created table and drop old one;
        action_plan.append(['ADVISE', f"-- insert into  {_table_owner}.{_table_name} select * from {_table_owner}.{_table_name}_old;"])
        action_plan.append(['ADVISE', f"-- drop table {_table_owner}.{_table_name}_old;"])

        logger.info("Action items and step by step plan below:")
        advise_file = open(f"{_table_owner}_{_table_name}.txt", "w")
        for action_item in action_plan:
            if action_item[0] == 'ERROR':
                logger.error(action_item[1])
            else:
                logger.info(action_item[1])
            advise_file.write("\n" + str(action_item[1]) + "\n")
        advise_file.close()
        # print statements in file
    except OSError as e:
        errno, strerr = e.args
        logger.critical("OS error({0}): {1}".format(errno, strerr))
    except Exception as e:
        logger.critical("Other error: {0}".format(e.args[0]))


if __name__ == "__main__":
    sys.exit(main())

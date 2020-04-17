import sys
from loguru import logger
from OracleHelper import OracleHelper
import sqls
########################################################################################################################
def main() -> int:
    conn1_info = {
        "connection_type": "direct",
        "host_name": "localhost",
        "service_name": "XE",
        "port": "1521"
    }
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
    conn1._connect(username='test', password='test')
    src_table = ('TEST2', 'TEST')
    src_table_ddl = ('TABLE', 'TEST2', 'TEST')

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
        tbs_name = value[2][9]
        space_used = res[0][0]
        res = conn1.runSelect(sqls.sql['table_size'], idx_tuple)
        logger.info(f"And size is {space_used:,} in TBS {tbs_name}")
        usage[tbs_name] = usage.get(tbs_name, 0) + space_used

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

    # get ALL DDL's for table
    # let's prepare session before getting DDLs
    conn1.runPLSQL_noret(sqls.sql['prepare_extract_ddl'])

    res = conn1.runSelect(sqls.sql['object_ddl'], src_table_ddl)
    for item in res:
        logger.info(item[0])
        action_plan.append(['ADVISE', item[0]])

    for (key, value) in constraints.items():
        tpl_constraint = ('CONSTRAINT', value[0], value[1])
        res = conn1.runSelect(sqls.sql['object_ddl'], tpl_constraint)
        logger.info(res[0][0])
        action_plan.append(['ADVISE', res[0][0]])

    for item in all_indexes:
        tpl_index = ('INDEX', item[0], item[1])
        res = conn1.runSelect(sqls.sql['object_ddl'], tpl_index)
        logger.info(res[0][0])
        action_plan.append(['ADVISE', res[0][0]])

    for item in all_triggers:
        tpl_trigger = ('TRIGGER', item[0], item[1])
        res = conn1.runSelect(sqls.sql['object_ddl'], tpl_trigger)
        action_plan.append(['ADVISE', res[0][0]])

    logger.info("Action items and step by step plan below:")
    for action_item in action_plan:
        logger.debug('-------------------------------------')
        if action_item[0] == 'ERROR':
            logger.error(action_item[1])
        else:
            logger.info(action_item[1])

    return 0
    # not it is time to get indexes sized
    # this is end of script debug print
    logger.debug("*" * 79)
    logger.debug("Table constraints")
    logger.debug(constraints)
    logger.debug("Pre-created indexes")
    logger.debug(auto_created_indexes)
    logger.debug("Indexes")
    logger.debug(indexes)
    logger.debug("Usage per TBS")
    logger.debug(usage)

    ####
    return 0
    ########################################################################################################################
    ########################################################################################################################
    ########################################################################################################################

    res = conn1.runSelect(sqls.sql['all_tables'], src_table_ddl)
    for item in res:
        print(item)

    # get all constraints
    res = conn1.runSelect(sqls.sql['all_constraints'], )
    all_constraints = {}

    res = conn1.runSelect(sqls.sql['object_ddl'], src_table)
    for item in res:
        print(item[0])

    src_index = ('CONSTRAINT', 'TEST2', 'TEST_PK')
    res = conn1.runSelect(sqls.sql['object_ddl'], src_index)
    for item in res:
        print(item[0])

    src_index = ('CONSTRAINT', 'TEST2', 'ck_check_val')
    res = conn1.runSelect(sqls.sql['object_ddl'], src_index)
    for item in res:
        print(item[0])


if __name__ == "__main__":
    sys.exit(main())

import cx_Oracle

class OracleHelper:
    def __init__(self, connection_info, logger, **kwargs) -> None:
        self.connection_info = connection_info
        self.logger = logger
        self.connection = None
        self.db = None
        self.dsn = None
        self.cursor = None
        self.logger.info("OracleHelper initialized")
        self.debug_sql = kwargs.get('debug_sql',False)
        if self.debug_sql:
            self.logger.debug("Creating Oracle Help Object with SQL debbugging option.")
        try:
            self.logger.debug(connection_info)
        except Exception as e:
            self.logger.critical(f"cx_Oracle error: {e.args[0]}")


    def _connect(self, **kwargs):
        try:
            # 2DO: add check for connection type and change connection style depending on this
            if self.connection_info.get("connection_type", 'tns') == 'direct':
                # check if there SID or SERVICE NAME and create DSN accordingly
                if 'service_name' in self.connection_info:
                    self.dsn = cx_Oracle.makedsn(host=self.connection_info['host_name'], port=self.connection_info['port'],
                                                 service_name=self.connection_info['service_name'])
                elif 'sid' in self.connection_info:
                    self.dsn = cx_Oracle.makedsn(host=self.connection_info['host_name'], port=self.connection_info['port'],
                                                 sid=self.connection_info['sid'])
                self.logger.debug(f"Current DSN: {self.dsn}")
                self.connection = cx_Oracle.connect(kwargs['username'], kwargs['password'], self.dsn,
                                                          encoding='UTF-8')
            elif connection_info["connection_type"] == 'tnsnames':
                self.connection = cx_Oracle.connect(kwargs['username'], kwargs['password'],
                                                          connection_info['connection_name'], encoding='UTF-8')
            else:
                logger.critical("Not specified or wrong connection mode (you can use direct/tnsnames")

            self.logger.debug("Connected to Oracle")
            self.cursor = self.connection.cursor()
            self.logger.info("Oracle connection created ")
        except Exception as e:
            self.logger.critical(f"cx_Oracle error: {e.args[0]}")


    def runSelect(self, sql, params) -> list:
        self.logger.debug(f"Number of parameters: {len(params)}")
        if self.debug_sql:
            self.logger.debug(f"Running SQL [ sql]: {sql['sql'].format(*params)}")
            self.logger.debug(f"Running SQL [desc]: {sql['desc']}")
        try:
            self.cursor.execute(sql['sql'].format(*params))
            res = [list(item) for item in self.cursor]
            if self.debug_sql:
                self.logger.debug(f"Total number of lines returned: {len(res)}")
                
            return res
        except Exception as e:
            self.logger.critical(f"cx_Oracle error: {e.args[0]}")
            exit(-1)

    def runPLSQL_noret(self, sql)->None:
        if self.debug_sql:
            self.logger.debug(f"Running SQL [ sql]: {sql['sql']}")
            self.logger.debug(f"Running SQL [desc]: {sql['desc']}")
        self.cursor.execute(sql['sql'])
        return None

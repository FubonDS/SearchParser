import psycopg2
import traceback
import logging
import configparser


class PostgresHandler:

    def __init__(
        self, config_path='./configs/private/database.ini', section='postgresql', logger=None
    ):
        self.config = self.load_db_config(filename=config_path, section=section)
        self.host = self.config['host']
        self.port = self.config['port']
        self.dbname = self.config['dbname']
        self.user = self.config['user']
        self.password = self.config['password']
        self.connection = None
        self.cursor = None
        self.table_header_dict = {}

        if logger is None:
            self.logger = logging.getLogger("PostgresHandler")
            self.logger.addHandler(logging.NullHandler())
        else:
            self.logger = logger

        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                connect_timeout=5,
            )
            self.cursor = self.connection.cursor()
            self.logger.info(f"[PostgresHandler] Connected to the database.")
        except Exception as e:
            self.logger.error(
                f"[PostgresHandler] Failed to connect to the database: {e}"
            )
            self.connection = None  
            self.cursor = None

    def load_db_config(self, filename='./configs/private/database.ini', section='postgresql'):
        parser = configparser.ConfigParser()
        parser.read(filename)
        
        db = {}
        if parser.has_section(section):
            items = parser.items(section)
            db = {item[0]: item[1] for item in items}
        else:
            raise Exception(f'Section {section} not found in the {filename} file.')
        
        return db

    def _execute_sql(self, sql, entries=[], multiple=False):

        result = {
            "indicator": False,
            "message": "",
            "header": [],
            "data": [],
            "formatted_data": [],
        }
        if self.connection is None or self.connection.closed:
            self.logger.error("[PostgresHandler] Database connection is not available.")
            result["message"] = "Database connection is not available."
            return result
        
        try:
            c = self.connection.cursor()

            # execute sql
            if multiple:
                c.executemany(sql, entries)
            else:
                if entries == []:
                    c.execute(sql)
                else:
                    c.execute(sql, entries)

            if c.description:
                result["data"] = c.fetchall()
                result["header"] = [desc[0] for desc in c.description]
                result["formatted_data"] = [
                    {result["header"][i]: value for i, value in enumerate(row)}
                    for row in result["data"]
                ]
            else:
                self.connection.commit() 

            result["indicator"] = True
            result["message"] = "Operation succeeded."
            self.logger.info(
                f'[PostgresHandler] execute_sql Success: {result["message"]}'
            )

        except Exception as e:
            traceback.print_exc()
            self.logger.warning(f"[PostgresHandler] execute_sql Error: {e}")
            result["message"] = str(e)
            self.connection.rollback()

        return result

    def get_header(self, table_name, force=False, no_ser_pk=False):

        if table_name in self.table_header_dict and not force:
            return self.table_header_dict[table_name]
        else:
            table_info = table_name.split(".")  # ex: 'public.inventory'
            if len(table_info) == 2:
                schema_name = table_info[0]
                table_name = table_info[1]
            else:
                schema_name = "public"
                table_name = table_info[0]

        sql_cmd = "SELECT column_name FROM information_schema.columns WHERE (table_schema = '{}') AND (table_name = '{}')".format(
            schema_name, table_name
        )
        if no_ser_pk:
            sql_cmd += " AND (column_default NOT LIKE 'nextval%' OR column_default IS NULL) AND (generation_expression IS NULL)"
        result = self._execute_sql(sql_cmd)
        return [item[0] for item in result["data"]]

    def get_data(
        self,
        table,
        target_column_list=[],
        conditional_rule_list=[],
        order_by_list=[],
        limit_number=-1,
    ):

        try:
            sql_cmd = "SELECT "
            if not target_column_list:
                sql_cmd += f"* FROM {table} "
            else:
                sql_cmd += f"{', '.join(target_column_list)} FROM {table} "

            entries = []
            if conditional_rule_list:
                sql_cmd += "WHERE "
                conditions = []
                for condition_rule in conditional_rule_list:
                    if 'IN' in condition_rule[0]:
                        conditions.append(f"{condition_rule[0]}")
                        entries.extend(condition_rule[1])
                    else:
                        conditions.append(f"{condition_rule[0]} %s")
                        entries.append(condition_rule[1])
                sql_cmd += " AND ".join(conditions) + " "

            if order_by_list:
                sql_cmd += "ORDER BY " + ", ".join(order_by_list) + " "

            if limit_number > 0:
                sql_cmd += f"LIMIT {limit_number} "

            sql_cmd += ";"
            self.logger.info(
                f"[PostgresHandler] get_data sql command: {sql_cmd}, entries: {entries}"
            )
            if entries == []:
                result = self._execute_sql(sql_cmd)
            else:
                result = self._execute_sql(sql_cmd, entries)

            return result
        except Exception as e:
            msg = "[PostgresHandler] get_data ERROR: " + str(e)
            self.logger.error(msg)
            return {
                "indicator": False,
                "message": msg,
                "header": [],
                "data": [],
                "formatted_data": [],
            }

    def update_data(self, table, editing_list, reference_column_list):

        if editing_list == []:
            return {"indicator": True, "message": "Editing list is empty."}
        if reference_column_list == []:
            return {"indicator": False, "message": "Reference column list is empty."}
        if not all(
            [
                set(reference_column_list).issubset(list(data.keys()))
                for data in editing_list
            ]
        ):
            return {
                "indicator": False,
                "message": "Reference column list is not subset of editing list.",
            }

        try:
            entries = []
            sql_cmd = f"UPDATE {table} SET "

            set_clauses = {
                f"{key} %s"
                for key in editing_list[0]
                if key not in reference_column_list
            }
            sql_cmd += ", ".join(set_clauses) + " WHERE "

            where_clauses = {f"{key} %s" for key in reference_column_list}
            sql_cmd += " AND ".join(where_clauses) + ";"

            for data in editing_list:
                entries = [
                    data[key] for key in data if key not in reference_column_list
                ]
                entries += [data[key] for key in data if key in reference_column_list]

            self.logger.info(
                f"[PostgresHandler] update_data sql command: {sql_cmd}, entries: {entries}"
            )
            result = self._execute_sql(sql_cmd, entries)
            del result["header"]
            del result["data"]
            return result
        except Exception as e:
            msg = "[PostgresHandler] update_data ERROR: " + str(e)
            self.logger.error(msg)
            return {"indicator": False, "message": msg}

    def add_data(self, table, adding_list, adding_header_list=[], to_null=False, on_conflict_do_nothing=True, unique_columns=None):

        if adding_list == []:
            return {"indicator": True, "message": "Adding list is empty."}
        if adding_header_list == []:
            adding_header_list = self.get_header(table, no_ser_pk=True)

        try:
            column_name_cmd = ""
            value_cmd = ""
            for header in adding_header_list:
                column_name_cmd += header + ", "
                value_cmd += "%s, "
            column_name_cmd = column_name_cmd[:-2]
            value_cmd = value_cmd[:-2]
            
            on_conflict_cmd = ""
            if on_conflict_do_nothing and unique_columns:
                on_conflict_cmd = f" ON CONFLICT ({unique_columns}) DO NOTHING"
                sql_cmd = f"INSERT INTO {table} ({column_name_cmd}) VALUES ({value_cmd}){on_conflict_cmd};"
            else:
                sql_cmd = f"INSERT INTO {table} ({column_name_cmd}) VALUES ({value_cmd});"
            entries = []

            for data in adding_list:
                entries.append([])
                for key in adding_header_list:
                    if key in data:
                        entries[-1].append(data[key])
                    elif key == "search_vector_en":  # 讓 search_vector_en 預設為 None
                        entries[-1].append(None)
                    elif to_null:
                        entries[-1].append(None)
                    else:
                        raise Exception("Lack of Data ( {} ): {}".format(table, key))
            self.logger.info(
                f"[PostgresHandler] add_data sql command: {sql_cmd}, entries: {entries}"
            )
            result = self._execute_sql(sql_cmd, entries, multiple=True)
            del result["header"]
            del result["data"]
            return result
        except Exception as e:
            msg = "[PostgresHandler] add_data ERROR: " + str(e)
            self.logger.error(msg)
            return {"indicator": False, "message": msg}

    def delete_data(self, table, filter_list, reference_column_list):

        if filter_list == []:
            return {"indicator": True, "message": "Filter list is empty."}
        if reference_column_list == []:
            return {"indicator": False, "message": "No reference."}
        if not all(
            [
                set(reference_column_list).issubset(list(data.keys()))
                for data in filter_list
            ]
        ):
            return {
                "indicator": False,
                "message": "Reference column list is not subset of filter list.",
            }

        try:
            sql_cmd = f"DELETE FROM {table} WHERE "
            entries = []
            for column in reference_column_list:
                sql_cmd += f"{column} %s AND "
            sql_cmd = sql_cmd[:-4] + ";"

            for data in filter_list:
                entries.append([])
                for column in reference_column_list:
                    entries[-1].append(data[column])
            self.logger.info(
                f"[PostgresHandler] delete_data sql command: {sql_cmd}, entries: {entries}"
            )
            result = self._execute_sql(sql_cmd, entries, multiple=True)
            del result["header"]
            del result["data"]
            return result
        except Exception as e:
            msg = "[PostgresHandler] delete_data ERROR: " + str(e)
            self.logger.error(msg)
            return {"indicator": False, "message": msg}

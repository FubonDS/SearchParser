import configparser
import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker


class DBHandler:
    def __init__(
        self, 
        config_path: str,
        section: str = "sqlite"
    ):
        self.logger = self._setup_logger()
        self.config = self.load_db_config(config_path, section)
        self.logger.info(f"[DBHandler] Loaded DB config for section [{section}]")
        
        self.engine_type = self.config.get("engine", "sqlite").lower()
        
        self.database_url = self._construct_database_url()
        self.logger.info(f"[DBHandler] Constructed database URL: {self.database_url}")
        
        self.precheck_database_exists()
        
        try:
            self.engine = create_engine(self.database_url, echo=True)
            self.SessionLocal = sessionmaker(bind=self.engine)
            self.logger.info("[DBHandler] Database engine and sessionmaker created successfully.")
        except Exception as e:
            self.logger.error(f"[DBHandler] Error creating database engine: {e}")
            raise e
        
        self.table_header_dict = {}
        
    def get_header(self, table_name: str) -> list:
        try:
            if table_name in self.table_header_dict:
                self.logger.info(f"[DBHandler] Retrieved cached headers for table '{table_name}'")
                return self.table_header_dict[table_name]
            
            with self.engine.connect() as conn:
                if self.engine_type == "sqlite":
                    query = text(f"PRAGMA table_info({table_name})")
                else:
                    self.logger.error(f"[DBHandler] get_header not implemented for engine type: {self.engine_type}")
                    return []

                result = conn.execute(query)

                if self.engine_type == "sqlite":
                    headers = [row[1] for row in result.fetchall()]
                else:
                    headers = []

                self.table_header_dict[table_name] = headers
                self.logger.info(f"[DBHandler] Retrieved headers for table '{table_name}': {headers}")
                return headers

        except Exception as e:
            self.logger.error(f"[DBHandler] Error retrieving headers for table '{table_name}': {e}")
            return []

                 
    def precheck_database_exists(self):
        if self.engine_type == "sqlite":
            db_path = self.config.get("filepath", "./local.db")
            if not os.path.exists(db_path):
                self.logger.warning(f"[DBHandler] Please remember to create tables in the new database at {db_path}")
            else:
                self.logger.info(f"[DBHandler] SQLite database file already exists at {db_path}")
        else:
            self.logger.warning(f"[DBHandler] Precheck for engine type '{self.engine_type}' is not implemented.")
    
    def _execute_sql(self, sql, entries=[]):
        
        result_dict = {
            "indicator": False,
            "message": "",
            "header": [],
            "data": [],
            "formatted_data": [],
        }
        
        try:
            with self.engine.begin() as conn:  # 自動 commit
                if entries:
                    result = conn.execute(text(sql), entries).fetchall()
                else:
                    result = conn.execute(text(sql))
            self.logger.info(f"[DBHandler] Successfully executed SQL: {sql}")
            
            data = [dict(row._mapping) for row in result]
            result_dict["indicator"] = True
            result_dict["message"] = "SQL executed successfully."
            result_dict["data"] = data
            result_dict["formatted_data"] = data
            
            return result_dict
        except Exception as e:
            self.logger.error(f"[DBHandler] Error executing SQL: {sql} with error: {e}")
            result_dict["message"] = str(e)
            return result_dict
        
    def get_data(
        self,
        table: str,
        target_column_list: list = None,
        conditional_rule_list: list = None,
        order_by_list: list = None
    ) -> dict:
        result_dict = {
            "indicator": False,
            "message": "",
            "header": [],
            "data": [],
            "formatted_data": [],
        }
        
        try:
            """
            ex : 
                target_column_list = ["id", "url", "title"]
                column_sql = "id, url, title"
            """
            if not target_column_list:
                column_sql = "*"
            else:
                column_sql = ", ".join(target_column_list)
                
            where_clause = []
            params = {}
            
            if conditional_rule_list:
                for idx, rule in enumerate(conditional_rule_list):
                    """
                    [
                        ("column_name", "=", value),
                    ]
                    """
                    col = rule[0]
                    op = rule[1]
                    val = rule[2]
                    
                    param_key = f"param_{idx}"
                    
                    if op == "IN" and isinstance(val, list):
                        where_clause.append(f"{col} IN :{param_key}")
                        params[param_key] = tuple(val)
                    else:
                        where_clause.append(f"{col} {op} :{param_key}")
                        params[param_key] = val
                                
            where_sql = ""
            """
            ex : where_sql = " WHERE id = :id AND title = :title"
            params = {"id": 1, "title": "Some Title"}
            """
            if where_clause:
                where_sql = " WHERE " + " AND ".join(where_clause)
            
            order_sql = ""
            if order_by_list:
                """
                order_by_list = ["id DESC", "published ASC"]
                """
                order_sql = " ORDER BY " + ", ".join(order_by_list)
            
            sql = f"SELECT {column_sql} FROM {table}{where_sql}{order_sql}"
            query = text(sql)
            
            self.logger.info(f"[DBHandler] Executing SQL: {sql} with params: {params}")
            
            with self.engine.begin() as conn:
                rows = conn.execute(query, params).fetchall()
                
            header = self.get_header(table) if target_column_list is None else target_column_list
            
            data = [dict(row._mapping) for row in rows]
            result_dict["indicator"] = True
            result_dict["message"] = f"Fetched {len(data)} rows from {table}"
            result_dict["header"] = list(header)
            result_dict["data"] = data
            result_dict["formatted_data"] = data  # 未來你可自訂格式

            return result_dict
        
        except Exception as e:
            error_msg = f"[DBHandler] get_data() error: {e}"
            self.logger.error(error_msg)
            result_dict["message"] = error_msg
            return result_dict
        
    def add_data(
        self,
        table: str,
        adding_list: list,
        adding_header_list: list = None,
        to_null: bool = False,
        auto_generated_columns = {"id"},
        on_conflict_do_nothing=True, 
        unique_columns=None
    ):
        result_dict = {
            "indicator": False,
            "message": "",
            "header": [],
            "data": [],
            "formatted_data": [],
        }
        
        try:
            if not adding_list:
                result_dict['message'] = "No data to add."
                return result_dict
            
            table_columns = self.get_header(table)
            result_dict['header'] = table_columns
            
            if not adding_header_list:
                adding_header_list = [
                    col for col in table_columns if col not in auto_generated_columns
                ]
            else:
                # 即使呼叫者有傳，也幫他過濾掉自動欄位
                adding_header_list = [
                    col for col in adding_header_list if col not in auto_generated_columns
                ]
                
            cleaned_rows = []
            
            for idx, row in enumerate(adding_list):
                cleaned_row = {}
                
                for col in adding_header_list:
                    if col in row:
                        cleaned_row[col] = row[col]
                    else:
                        if to_null:
                            cleaned_row[col] = None
                        else:
                            result_dict['message'] = (
                                f"Row {idx} is missing column '{col}' and to_null is False."
                            )
                            return result_dict
                        
                cleaned_row = {
                    k: v for k, v in cleaned_row.items() if k in table_columns
                }
                
                cleaned_rows.append(cleaned_row)    
                
            if not cleaned_rows:
                result_dict["message"] = "No valid rows to insert."
                return result_dict

            # 用第一筆的 key 當作欄位順序（假設所有 cleaned_row 欄位一致）
            sql_columns = ", ".join(cleaned_rows[0].keys())
            sql_placeholders = ", ".join([f":{k}" for k in cleaned_rows[0].keys()])
            
            on_conflict_cmd = ""
            if on_conflict_do_nothing and unique_columns:
                on_conflict_cmd = f" ON CONFLICT ({unique_columns}) DO NOTHING"
                sql = f"INSERT INTO {table} ({sql_columns}) VALUES ({sql_placeholders}){on_conflict_cmd}"
            else:
                sql = f"INSERT INTO {table} ({sql_columns}) VALUES ({sql_placeholders})"
            
            query = text(sql)

            # 一次性批次插入
            with self.engine.begin() as conn:  # 自動 commit
                conn.execute(query, cleaned_rows)  # ← 這裡放的是 cleaned_rows（list of dict）

            result_dict['indicator'] = True
            result_dict['message'] = f"Inserted {len(cleaned_rows)} rows into {table}."
            result_dict['data'] = cleaned_rows
            result_dict['formatted_data'] = cleaned_rows

            return result_dict
        
        except Exception as e:
            error_msg = f"[DBHandler] add_data() error: {e}"
            self.logger.error(error_msg)
            result_dict["message"] = error_msg
            return result_dict         
                         
    def delete_data(
        self,
        table: str,
        filter_list, 
        reference_column_list
    ) -> dict:
        result_dict = {
            "indicator": False,
            "message": "",
            "header": reference_column_list or [],
            "data": filter_list,
            "formatted_data": filter_list,
        }
        
        try:
            if not reference_column_list:
                result_dict["message"] = "reference_column_list cannot be empty."
                return result_dict

            if not filter_list:
                result_dict["message"] = "filter_list is empty, no rows to delete."
                return result_dict
            
            table_columns = self.get_header(table)
            missing_cols = [c for c in reference_column_list if c not in table_columns]
            if missing_cols:
                result_dict["message"] = (
                    f"Columns {missing_cols} not found in table '{table}'."
                )
                return result_dict
            
            where_clauses = []
            params = {}

            for idx, row in enumerate(filter_list):
                sub_clauses = []
                for col in reference_column_list:
                    if col not in row:
                        result_dict["message"] = (
                            f"Row {idx} is missing column '{col}' in filter_list."
                        )
                        return result_dict
                    param_key = f"{col}_{idx}" 
                    sub_clauses.append(f"{col} = :{param_key}")
                    params[param_key] = row[col]
                
                where_clauses.append("(" + " AND ".join(sub_clauses) + ")")
            where_sql = " OR ".join(where_clauses)
            
            sql = f"DELETE FROM {table} WHERE {where_sql}"
            query = text(sql)
            
            with self.engine.begin() as conn:
                result = conn.execute(query, params)
                deleted_count = result.rowcount
            
            result_dict["indicator"] = True
            result_dict["message"] = f"Deleted {deleted_count} rows from {table}."
            return result_dict
        except Exception as e:
            error_msg = f"[DBHandler] delete_data() error: {e}"
            self.logger.error(error_msg)
            result_dict["message"] = error_msg
            return result_dict
        
    def get_session(self) -> Session:
        return self.SessionLocal()
    
    def create_tables(self):
        from .models import Base
        Base.metadata.create_all(self.engine)
        self.logger.info("[DBHandler] All tables created successfully.")
        
    def _construct_database_url(self) -> str:
        if self.engine_type == "sqlite":
            filepath = self.config.get("filepath", "./local.db")
            return f"sqlite:///{filepath}"
        else:
            self.logger.error(f"[DBHandler] Unsupported database engine: {self.engine_type}")
            raise ValueError(f"Unsupported database engine: {self.engine_type}")
        
    def load_db_config(self, config_path: str, section: str) -> dict:
        parser = configparser.ConfigParser()
        parser.read(config_path)
        
        if not parser.has_section(section):
            raise Exception(f"Section [{section}] not found in {config_path}")
        
        return {key: parser.get(section, key) for key in parser[section]}
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.__class__.__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            ))
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
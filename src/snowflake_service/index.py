import snowflake.connector


class Snowflake:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user="tngiabao",
            password="Giabao@22102652210265",
            account="iw82368.ap-southeast-1",
            warehouse="COMPUTE_WH",
            database="DBMS",
            schema="PUBLIC",
        )
        self.cursor = self.conn.cursor()
        print("Connected to Snowflake")

    def execute(self, command: str, params: tuple | list = ()):
        # print(f"Executing command: {command} with params: {params}")

        try:
            self.cursor.execute(command, params)
        except Exception as e:
            print(f"Error executing query: {e}")
            
            return None

    def close_connection(self):
        self.conn.close()

    def insert_data(self, table, data):
        cursor = self.conn.cursor()
        try:
            placeholders = ", ".join(["%s"] * len(data))
            sql = f"INSERT INTO {table} VALUES ({placeholders})"
            cursor.execute(sql, data)
            self.conn.commit()
        finally:
            cursor.close()


snowflake_service = Snowflake()

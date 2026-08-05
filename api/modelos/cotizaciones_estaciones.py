import pyodbc
from api.db_connections import CONTROLGASTG_CONN_STR


class CotizacionesEstaciones:
    def __init__(self, conn_str: str = CONTROLGASTG_CONN_STR):
        self.conn_str = conn_str

    def get_last_exchange(self, linked_server, short_db, codgas, station_name, no_station, description):
        codgas = int(codgas)

        # Escapamos comillas simples para los literales que van dentro de OPENQUERY.
        station_name_sql = str(station_name).replace("'", "''")
        no_station_sql = str(no_station).replace("'", "''")
        description_sql = str(description).replace("'", "''")

        query = f"""
        WITH cte AS (
            SELECT *,
                 CAST(CONVERT(VARCHAR(100), CAST(fch AS DATETIME) - 1, 23) AS VARCHAR(10)) AS Fecha
            FROM (
                SELECT TOP (1) [codmda], [codgas], [fch], CONCAT(RIGHT('00' + CAST(FLOOR(hra / 100) AS VARCHAR(2)), 2), ':', RIGHT('00' + CAST(hra % 100 AS VARCHAR(2)), 2)) AS hra_format, [hra], [ctz], [ctzcom], [ctzven], [codpza], [codcpo], [logusu], [logfch], [lognew], N'{station_name_sql}' AS station_name, N'{no_station_sql}' AS no_station, N'{description_sql}' AS description
                FROM OPENQUERY({linked_server}, '
                    SELECT TOP (1) [codmda], [codgas], [fch], [hra], [ctz], [ctzcom], [ctzven], [codpza], [codcpo], [logusu], [logfch], [lognew] FROM {short_db}.[Cotizaciones]
                    WHERE codgas = {codgas} ORDER BY lognew DESC
                ')
            ) AS inner_cte
        )
        SELECT * FROM cte;
        """

        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cols = [col[0] for col in cursor.description]
                row = cursor.fetchone()
            if row is None:
                return None
            return dict(zip(cols, row))
        except pyodbc.Error as e:
            print(f"CotizacionesEstaciones error para estacion {codgas} ({station_name}): {e}")
            return None

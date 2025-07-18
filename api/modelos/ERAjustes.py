import pyodbc
from api.db_connections import CONTROLGAS_CONN_STR

class ERAjustes:
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str

    def get_erajustes(self, date: str = '2025-01-01'):
        sql = """
       SELECT
            empresa AS Empresa,
            centro_costo AS CentroCosto,
            estado_resultados AS CatCentroCosto,
            no_cuenta AS NoCuenta,
            rubro AS Rubro,
            concepto AS Concepto,
            enero AS Enero,
            febrero AS Febrero,
            marzo AS Marzo,
            abril AS Abril,
            mayo AS Mayo,
            junio AS Junio,
            julio AS Julio,
            agosto AS Agosto,
            septiembre AS Septiembre,
            octubre AS Octubre,
            noviembre AS Noviembre,
            diciembre AS Diciembre,
            'ajustes' as origin
        FROM [TGV2].[dbo].[ERAjustes]
            WHERE YEAR(fecha) = ?;
        """
        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, date)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []


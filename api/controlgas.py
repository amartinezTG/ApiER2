import pyodbc
from .db_connections import CONTROLGAS_CONN_STR

class ControlGas:
    def __init__(self, conn_str: str = CONTROLGAS_CONN_STR):
        self.conn_str = conn_str

    def get_er_budget(self, year: int = 2025):
        sql = """
        WITH cte AS (
            SELECT
                t1.name               AS Rubro,
                t2.name               AS Concepto,
                t2.categoria          AS Categoria,
                t3.qty                AS Cantidad,
                MONTH(t3.date_budget) AS Mes
            FROM TGV2.dbo.ERRubros AS t1
            JOIN TGV2.dbo.ERRubroConcept AS t2
                ON t1.id = t2.id_rubro
            JOIN TGV2.dbo.ERRubroBudget AS t3
                ON t2.id = t3.id_concept
            WHERE YEAR(t3.date_budget) = ?
            )
            SELECT
            Rubro,
            Concepto,
            Categoria,
            ISNULL([1],0)  AS Enero,
            ISNULL([2],0)  AS Febrero,
            ISNULL([3],0)  AS Marzo,
            ISNULL([4],0)  AS Abril,
            ISNULL([5],0)  AS Mayo,
            ISNULL([6],0)  AS Junio,
            ISNULL([7],0)  AS Julio,
            ISNULL([8],0)  AS Agosto,
            ISNULL([9],0)  AS Septiembre,
            ISNULL([10],0) AS Octubre,
            ISNULL([11],0) AS Noviembre,
            ISNULL([12],0) AS Diciembre
            FROM cte
            PIVOT (
            SUM(Cantidad)
            FOR Mes IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])
            ) AS p
            ORDER BY Rubro,  Enero desc;

        """

        try:
            with pyodbc.connect(self.conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, year)
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        except pyodbc.Error as e:
            # podrías usar logging en lugar de print
            print(f"ControlGas DB error: {e}")
            return []

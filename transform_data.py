import logging
import os
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
from typing import Union


# Verificar si la carpeta 'logs' existe, si no, créala
if not os.path.exists("logs"):
    os.makedirs("logs")

# Configuración del logger
logging.basicConfig(
    filename="logs/transform.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def to_datetime(date_str: str) -> Union[Timestamp, None]:
    """
    Convierte una cadena de fecha a un objeto datetime.
    Args:
        date: La cadena de fecha para convertir.
    Returns: El objeto datetime correspondiente o None si la conversión falla.
    """

    try:
        return pd.to_datetime(date_str)
    except ValueError:
        return None


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a DataFrame: strings to uppercase and removes duplicate rows.
    Args
        df: Original DataFrame
    Returns: Transformed DataFrame
    """
    try:
        # Unificamos la fechas
        df["fecha_respuesta"] = df["fecha_respuesta"].apply(pd.to_datetime)

        # Convertir todas las celdas string a mayúsculas
        df = df.apply(lambda x: x.str.upper() if x.dtype == "object" else x)

        # Obtener el índice de la fecha más reciente para cada user_id
        idx = df.groupby("user_id")["fecha_respuesta"].idxmax()

        # Filtrar el DataFrame usando el índice obtenido
        data_filtrada = df.loc[idx].sort_values(by="fecha_respuesta", ascending=False)

        # 1. Usuarios con más de 5 cuentas abiertas
        users_more_than_5_accounts = data_filtrada[
            data_filtrada["cuentas_abiertas"] > 5
        ]

        # 2. Usuarios con un score arriba de 650
        users_score_above_650 = data_filtrada[
            data_filtrada["score_buro_de_credito"] > 650
        ]

        # 3. Usuarios que tuvieron una respuesta en el mes de noviembre
        users_november_responses = data_filtrada[
            data_filtrada["fecha_respuesta"].dt.month == 11
        ]

        # 4. Usuarios con por lo menos 2 tarjetas de crédito y sin saldo actual
        users_2_credit_cards_no_balance = data_filtrada[
            (data_filtrada["tarjetas_de_credito"] >= 2)
            & (data_filtrada["saldo_actual"] == 0)
        ]

        num_users_more_than_5_accounts = users_more_than_5_accounts["user_id"].nunique()
        num_users_score_above_650 = users_score_above_650["user_id"].nunique()
        num_users_november_responses = users_november_responses["user_id"].nunique()
        num_users_2_credit_cards_no_balance = users_2_credit_cards_no_balance[
            "user_id"
        ].nunique()

        logger.info(
            f"Hay {num_users_more_than_5_accounts} usuarios únicos que tienen más de 5 cuentas abiertas"
        )
        logger.info(
            f"Hay {num_users_score_above_650} usuarios únicos que tienen un score en el buró de crédito mayor a 650."
        )
        logger.info(
            f"Hay {num_users_november_responses} usuarios únicos que tuvieron una respuesta en el mes de noviembre.."
        )
        logger.info(
            f"Hay {num_users_2_credit_cards_no_balance} usuarios únicos que tienen por lo menos 2 tarjetas de crédito y no tienen saldo."
        )

        return data_filtrada

    except Exception as e:
        logger.error(f"Error transforming file: {e}")

"""DynamoDB ヘルパー。

Streams レコード変換、型変換、テーブル参照取得をまとめた
薄いインフラユーティリティを提供する。
"""

from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

_deserializer = TypeDeserializer()
_serializer = TypeSerializer()


def deserialize_image(image: dict[str, Any]) -> dict[str, Any]:
    """DynamoDB Streams の NewImage / OldImage を Python dict に変換する。
    
    Args:
        image: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    if image is None:
        return {}
    return {k: _deserializer.deserialize(v) for k, v in image.items()}


def serialize_value(value: Any) -> dict:
    """Python の値を DynamoDB 形式に変換する。"""
    return _serializer.serialize(value)


def float_to_decimal(value: float) -> Decimal:
    """float を DynamoDB 互換の Decimal に変換する。
    
    Args:
        value: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    return Decimal(str(round(value, 4)))


def decimal_to_float(value: Decimal | float | int) -> float:
    """Decimal / int を float に変換する。"""
    return float(value)


def get_table(table_name: str):
    """DynamoDB Table リソースを取得する。
    
    Args:
        table_name: 引数。
    
    Returns:
        なし。
    
    Notes:
        なし。
    """
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(table_name)

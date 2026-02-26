"""shared/dynamodb.py の単体テスト"""

from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from shared.dynamodb import (
    decimal_to_float,
    deserialize_image,
    float_to_decimal,
    get_table,
    serialize_value,
)


class TestDeserializeImage:
    def test_none_returns_empty_dict(self):
        assert deserialize_image(None) == {}

    def test_valid_image(self):
        image = {
            "tenant_id": {"S": "t-001"},
            "count": {"N": "42"},
            "active": {"BOOL": True},
        }
        result = deserialize_image(image)
        assert result["tenant_id"] == "t-001"
        assert result["count"] == Decimal("42")
        assert result["active"] is True

    def test_empty_image(self):
        assert deserialize_image({}) == {}


class TestSerializeValue:
    def test_string(self):
        result = serialize_value("hello")
        assert result == {"S": "hello"}

    def test_number(self):
        result = serialize_value(42)
        assert result == {"N": "42"}

    def test_bool(self):
        result = serialize_value(True)
        assert result == {"BOOL": True}


class TestFloatToDecimal:
    def test_conversion(self):
        result = float_to_decimal(3.14159)
        assert isinstance(result, Decimal)
        assert result == Decimal("3.1416")

    def test_round_trip(self):
        original = 5.0
        dec = float_to_decimal(original)
        assert float(dec) == original


class TestDecimalToFloat:
    def test_decimal(self):
        assert decimal_to_float(Decimal("3.14")) == 3.14

    def test_int(self):
        assert decimal_to_float(42) == 42.0

    def test_float_passthrough(self):
        assert decimal_to_float(2.5) == 2.5


class TestGetTable:
    @mock_aws
    def test_returns_table_resource(self):
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
        dynamodb.create_table(
            TableName="TestTable",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table = get_table("TestTable")
        assert table.table_name == "TestTable"

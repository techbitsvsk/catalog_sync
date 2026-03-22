"""
tests/test_path_translator.py — Tests for the PathTranslator.
"""

import pytest
from iceberg_sync.path_translator import PathTranslator, aws_to_azure, StorageRoot


class TestStorageRoot:
    def test_trailing_slash_added(self):
        root = StorageRoot("s3://bucket/path")
        assert root.uri == "s3://bucket/path/"

    def test_trailing_slash_preserved(self):
        root = StorageRoot("s3://bucket/path/")
        assert root.uri == "s3://bucket/path/"

    def test_scheme(self):
        assert StorageRoot("s3://bucket/").scheme == "s3"
        assert StorageRoot("abfss://c@a.dfs.core.windows.net/").scheme == "abfss"


class TestPathTranslator:
    @pytest.fixture
    def translator(self):
        return PathTranslator([
            ("s3://warehouse/iceberg/", "abfss://iceberg@acct.dfs.core.windows.net/iceberg/"),
            ("s3://raw-data/", "abfss://raw@acct.dfs.core.windows.net/"),
        ])

    def test_basic_translate(self, translator):
        result = translator.translate(
            "s3://warehouse/iceberg/gold/top_customers/data/00001.parquet"
        )
        assert result == "abfss://iceberg@acct.dfs.core.windows.net/iceberg/gold/top_customers/data/00001.parquet"

    def test_translate_metadata_path(self, translator):
        result = translator.translate(
            "s3://warehouse/iceberg/gold/top_customers/metadata/v3.metadata.json"
        )
        assert result == "abfss://iceberg@acct.dfs.core.windows.net/iceberg/gold/top_customers/metadata/v3.metadata.json"

    def test_translate_second_mapping(self, translator):
        result = translator.translate("s3://raw-data/tpch/orders.csv")
        assert result == "abfss://raw@acct.dfs.core.windows.net/tpch/orders.csv"

    def test_unknown_uri_strict(self, translator):
        with pytest.raises(ValueError, match="does not match any known source root"):
            translator.translate("s3://unknown-bucket/file.txt")

    def test_unknown_uri_non_strict(self, translator):
        result = translator.translate("s3://unknown-bucket/file.txt", strict=False)
        assert result == "s3://unknown-bucket/file.txt"

    def test_translate_many(self, translator):
        uris = [
            "s3://warehouse/iceberg/a.parquet",
            "s3://warehouse/iceberg/b.parquet",
        ]
        results = translator.translate_many(uris)
        assert all(r.startswith("abfss://") for r in results)

    def test_reverse(self, translator):
        rev = translator.reverse()
        result = rev.translate(
            "abfss://iceberg@acct.dfs.core.windows.net/iceberg/gold/data/00001.parquet"
        )
        assert result == "s3://warehouse/iceberg/gold/data/00001.parquet"

    def test_relative_path(self, translator):
        rel = translator.relative_path(
            "s3://warehouse/iceberg/gold/data/00001.parquet"
        )
        assert rel == "gold/data/00001.parquet"

    def test_first_match_wins(self):
        t = PathTranslator([
            ("s3://bucket/specific/path/", "abfss://c@a.dfs.core.windows.net/specific/"),
            ("s3://bucket/", "abfss://c@a.dfs.core.windows.net/general/"),
        ])
        result = t.translate("s3://bucket/specific/path/file.txt")
        assert "specific/" in result
        assert "general/" not in result


class TestFactories:
    def test_aws_to_azure(self):
        t = aws_to_azure(
            "s3://warehouse/iceberg/",
            "abfss://iceberg@acct.dfs.core.windows.net/iceberg/",
        )
        result = t.translate("s3://warehouse/iceberg/gold/data.parquet")
        assert result.startswith("abfss://")

    def test_aws_to_azure_with_additional(self):
        t = aws_to_azure(
            "s3://warehouse/iceberg/",
            "abfss://iceberg@acct.dfs.core.windows.net/iceberg/",
            additional_mappings=[("s3://raw/", "abfss://raw@acct.dfs.core.windows.net/")],
        )
        assert t.translate("s3://raw/data.csv").startswith("abfss://raw@")

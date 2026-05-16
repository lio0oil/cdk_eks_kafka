from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3tables as s3tables
from constructs import Construct

from ekscdk.config import ClusterConfig


class S3TablesStack(Stack):
    """consumer (EMR Spark Structured Streaming) が使う AWS リソース群。

    案 R-1 (テーブル分割 + 並列 streaming query) の構造に従って、
    schema_name (ProtoBuf message 名) ごとに別 Iceberg テーブルを定義する。

    現状はサンプルとして Event 1 種類のみ:
      - sample_events_event       (id, datetime, schema_name, rawdata)
      - sample_events_dlq         (failed_at, schema_name, rawdata, reason) ※全 schema 共通

    schema_name 列はどの ProtoBuf 型由来かを行ごとに記録する。
      - event 側: consumer が Kafka header の proto-schema 列をそのまま流す (route=ok のみが
                  入るため必ず非 NULL → required=True)
      - DLQ 側:   consumer が Kafka header の proto-schema 列をそのまま流す。
                  missing_schema 行 (header に proto-schema が無い失敗) は NULL になるため
                  required=False。NULL の意味は reason 列で識別できる。

    新しい ProtoBuf 型を追加する場合 (例えば Notification):
      1. kafka/proto/notification.proto を作って events.desc を再生成
      2. 本ファイルに sample_events_notification の CfnTable 定義を追加
      3. consumer 側は SCHEMAS が events.desc から自動展開されるのでコード変更不要

    Glue Data Catalog 統合 (`s3tablescatalog`) はアカウント・リージョン単位で 1 つの
    リソースのため本スタックでは作成しない。EMR Spark から S3TablesCatalog 経由で書き込む
    だけなら Glue 統合は不要。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        config: ClusterConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table_bucket = s3tables.CfnTableBucket(
            self,
            "TableBucket",
            table_bucket_name=config.s3_table_bucket_name,
        )
        table_bucket.apply_removal_policy(config.s3_table_bucket_removal_policy)

        namespace = s3tables.CfnNamespace(
            self,
            "EventsNamespace",
            table_bucket_arn=table_bucket.attr_table_bucket_arn,
            namespace="events",
        )
        namespace.add_dependency(table_bucket)

        # sample_events_event テーブル (Event スキーマ専用)。
        # consumer.py の foreachBatch では from_protobuf 結果の id / datetime と、
        # header 由来の proto_schema (= schema_name) と rawdata を明示 select して INSERT する。
        # day(datetime) パーティション・Copy-on-Write モードは前構成から踏襲。
        event_table = s3tables.CfnTable(
            self,
            "SampleEventsEventTable",
            table_bucket_arn=table_bucket.attr_table_bucket_arn,
            namespace=namespace.namespace,
            table_name="sample_events_event",
            open_table_format="ICEBERG",
            iceberg_metadata=s3tables.CfnTable.IcebergMetadataProperty(
                iceberg_schema=s3tables.CfnTable.IcebergSchemaProperty(
                    schema_field_list=[
                        s3tables.CfnTable.SchemaFieldProperty(id=1, name="id", type="long", required=True),
                        s3tables.CfnTable.SchemaFieldProperty(id=2, name="datetime", type="timestamp", required=True),
                        s3tables.CfnTable.SchemaFieldProperty(id=3, name="schema_name", type="string", required=True),
                        s3tables.CfnTable.SchemaFieldProperty(id=4, name="rawdata", type="binary", required=True),
                    ]
                ),
                iceberg_partition_spec=s3tables.CfnTable.IcebergPartitionSpecProperty(
                    fields=[
                        # 先頭は低カーディナリティ + 等値フィルタの identity(schema_name)。
                        # 続けて day(datetime) で時系列 prune。
                        s3tables.CfnTable.IcebergPartitionFieldProperty(
                            source_id=3,
                            transform="identity",
                            name="schema_name",
                            field_id=1000,
                        ),
                        s3tables.CfnTable.IcebergPartitionFieldProperty(
                            source_id=2,
                            transform="day",
                            name="datetime_day",
                            field_id=1001,
                        ),
                    ],
                ),
                table_properties={
                    "write.merge.mode": "copy-on-write",
                    "write.update.mode": "copy-on-write",
                    "write.delete.mode": "copy-on-write",
                },
            ),
        )
        event_table.add_dependency(namespace)

        # ProtoBuf デシリアライズ失敗データの退避先 (Dead Letter Queue)。
        # 全 schema_name 共通の 1 テーブル。失敗頻度は低い前提で 100 query が並行に書いても
        # Iceberg 楽観ロックの retry は許容範囲。
        dlq_table = s3tables.CfnTable(
            self,
            "SampleEventsDlqTable",
            table_bucket_arn=table_bucket.attr_table_bucket_arn,
            namespace=namespace.namespace,
            table_name="sample_events_dlq",
            open_table_format="ICEBERG",
            iceberg_metadata=s3tables.CfnTable.IcebergMetadataProperty(
                iceberg_schema=s3tables.CfnTable.IcebergSchemaProperty(
                    schema_field_list=[
                        s3tables.CfnTable.SchemaFieldProperty(id=1, name="failed_at", type="timestamp", required=True),
                        # header 由来の proto-schema をそのまま入れる。missing_schema 行 (header
                        # に proto-schema が無い失敗) は NULL になるため required=False。
                        s3tables.CfnTable.SchemaFieldProperty(id=2, name="schema_name", type="string", required=False),
                        s3tables.CfnTable.SchemaFieldProperty(id=3, name="rawdata", type="binary", required=True),
                        s3tables.CfnTable.SchemaFieldProperty(id=4, name="reason", type="string", required=True),
                    ]
                ),
                iceberg_partition_spec=s3tables.CfnTable.IcebergPartitionSpecProperty(
                    fields=[
                        # 先頭は schema 別の集計を等値 prune できる identity(schema_name)。
                        # 続けて day(failed_at) で時系列 prune (アラート / lifecycle)。
                        s3tables.CfnTable.IcebergPartitionFieldProperty(
                            source_id=2,
                            transform="identity",
                            name="schema_name",
                            field_id=1000,
                        ),
                        s3tables.CfnTable.IcebergPartitionFieldProperty(
                            source_id=1,
                            transform="day",
                            name="failed_at_day",
                            field_id=1001,
                        ),
                    ],
                ),
                table_properties={
                    "write.merge.mode": "copy-on-write",
                    "write.update.mode": "copy-on-write",
                    "write.delete.mode": "copy-on-write",
                },
            ),
        )
        dlq_table.add_dependency(namespace)

        # consumer の writeStream.option("checkpointLocation", ...) 用 S3 バケット。
        # 各 StreamingQuery は kafka-consumer-checkpoint-<account>-<env>/<schema_name>/
        # のように prefix で分割して使う (consumer/constants.py の checkpoint_for と一致)。
        is_destroyable = config.s3_table_bucket_removal_policy == RemovalPolicy.DESTROY
        checkpoint_bucket = s3.Bucket(
            self,
            "ConsumerCheckpointBucket",
            bucket_name=f"kafka-consumer-checkpoint-{self.account}-{config.s3_consumer_checkpoint_suffix}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=config.s3_table_bucket_removal_policy,
            auto_delete_objects=is_destroyable,
        )

        self._table_bucket_arn = table_bucket.attr_table_bucket_arn
        self._namespace_name = namespace.namespace
        self._event_table_name = "sample_events_event"
        self._dlq_table_name = "sample_events_dlq"
        self._checkpoint_bucket_name = checkpoint_bucket.bucket_name

        CfnOutput(self, "TableBucketArn", value=self._table_bucket_arn)
        CfnOutput(self, "NamespaceName", value=self._namespace_name)
        CfnOutput(self, "EventTableName", value=self._event_table_name)
        CfnOutput(self, "DlqTableName", value=self._dlq_table_name)
        CfnOutput(self, "CheckpointBucketName", value=self._checkpoint_bucket_name)

    @property
    def table_bucket_arn(self) -> str:
        return self._table_bucket_arn

    @property
    def namespace_name(self) -> str:
        return self._namespace_name

    @property
    def event_table_name(self) -> str:
        return self._event_table_name

    @property
    def dlq_table_name(self) -> str:
        return self._dlq_table_name

    @property
    def checkpoint_bucket_name(self) -> str:
        return self._checkpoint_bucket_name

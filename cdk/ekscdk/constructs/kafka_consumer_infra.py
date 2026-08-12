import os
import zipfile

from aws_cdk import RemovalPolicy
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_s3 as s3
from constructs import Construct

from ekscdk.config import ClusterConfig

# ekscdk/constructs/ から見た cdk/lambda/kafka_consumer/ のパス。
# KafkaConsumerAppStack の Code.from_bucket が参照するアーティファクトを作る際、
# scripts/ のアップロードスクリプトが build_lambda_zip 経由でここを読む。
LAMBDA_ASSET_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "lambda", "kafka_consumer")
LAMBDA_ZIP_PATH = os.path.join(LAMBDA_ASSET_DIR, "build", "function.zip")


def build_lambda_zip(source_dir: str, output_zip: str) -> str:
    """source_dir 直下のソースを決定論的な zip にまとめ、output_zip に書き出す。

    出力先（output_zip の親ディレクトリ）が source_dir の配下にあるため、2 回目以降の
    ビルドで前回生成した zip 自身を巻き込まないよう明示的に除外する。
    zipfile はデフォルトでエントリの mtime を保存するため、ソースが不変でも毎回
    バイト列が変わり、同一ソースからのアップロードのたびに無意味な差分が出る。
    date_time を固定してこれを避ける（ソース側の実際の更新日時は Lambda の動作に無関係）。
    """
    output_dir = os.path.normpath(os.path.dirname(output_zip))
    os.makedirs(output_dir, exist_ok=True)

    file_paths: list[str] = []
    for root, dirs, files in os.walk(source_dir):
        dirs[:] = [d for d in dirs if os.path.normpath(os.path.join(root, d)) != output_dir]
        file_paths.extend(os.path.join(root, name) for name in files)
    file_paths.sort()

    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in file_paths:
            arcname = os.path.relpath(path, source_dir)
            info = zipfile.ZipInfo(arcname, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            with open(path, "rb") as f:
                zf.writestr(info, f.read())

    return output_zip


class KafkaConsumerInfraConstruct(Construct):
    """Kafka Lambda コンシューマ（ESM）が使う AWS リソースのうち、インフラ寄りのもの。

    Lambda 関数本体・Event Source Mapping は KafkaConsumerAppStack が別スタックとして
    管理する（アプリケーションコードの更新頻度・承認フローがインフラ側と異なるため分離）。
    アーティファクトバケットへの zip アップロードは cdk deploy の外
    （scripts/ のアップロードスクリプト）で行う運用のため、ここでは空のバケットを
    用意するだけで Lambda 関数自体は作らない。

    - esm_sg: self-managed Kafka ESM のポーラー ENI 用 SG（Kafka NLB と AWS HTTPS API 以外への送信不可）
    - data_bucket: Lambda 実行時の設定・データ読み取り用（読み取り専用の grant は
      アプリ側が Lambda 実行ロールに対して行う）
    - artifact_bucket: Lambda デプロイパッケージ（zip）の格納先。バージョニング有効に
      し、KafkaConsumerAppStack は特定の VersionId を明示参照する
      （ClusterConfig.kafka_consumer_code_object_version 経由。S3Key だけでは
      CloudFormation がコード変更を検知できないため）。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        kafka_nlb_sg: ec2.ISecurityGroup,
        config: ClusterConfig,
    ) -> None:
        super().__init__(scope, construct_id)

        self._esm_sg = ec2.SecurityGroup(
            self,
            "KafkaEsmSg",
            vpc=vpc,
            allow_all_outbound=False,
            description="Self-managed Kafka ESM poller ENI (outbound to Kafka NLB and AWS HTTPS APIs only).",
        )
        # bootstrap 接続後、Kafka クライアントは各 partition のリーダー broker（advertised
        # port はブローカーごとに異なる、_manifest.py 参照）にも直接接続するため、
        # 単一ポートには絞れない。宛先を Kafka NLB の SG 参照に限定することで、
        # ポート番号を broker_count に追従させずに「NLB 以外への送信不可」を維持する。
        self._esm_sg.add_egress_rule(kafka_nlb_sg, ec2.Port.all_tcp())
        # ESM ポーラー ENI は Kafka broker への到達性に加え、Lambda invoke API と STS への
        # 到達性も必須（self-managed Kafka の VPC アクセス要件、AWS Lambda Developer Guide
        # with-kafka-permissions.html）。欠けると「your event source VPC must be able to
        # connect to Lambda and STS」で ESM が Kafka に接続できなくなる。NAT Gateway 越しに
        # 到達させる想定で、Lambda/STS には宛先を絞れる VPC エンドポイント用プレフィックス
        # リストが存在しないため 0.0.0.0/0:443 とする。
        self._esm_sg.add_egress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(443))

        is_destroyable = config.log_removal_policy == RemovalPolicy.DESTROY

        self._data_bucket = s3.Bucket(
            self,
            "KafkaLambdaConsumerDataBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=config.log_removal_policy,
            auto_delete_objects=is_destroyable,
        )

        self._artifact_bucket = s3.Bucket(
            self,
            "KafkaConsumerArtifactBucket",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=config.log_removal_policy,
            auto_delete_objects=is_destroyable,
        )

    @property
    def esm_sg(self) -> ec2.ISecurityGroup:
        return self._esm_sg

    @property
    def data_bucket(self) -> s3.IBucket:
        return self._data_bucket

    @property
    def artifact_bucket(self) -> s3.IBucket:
        return self._artifact_bucket

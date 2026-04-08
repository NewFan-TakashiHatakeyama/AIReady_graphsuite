"""
AWS アーキテクチャ図（AWS Architecture Icons 相当の埋め込み画像）を PNG で出力します。

利用ライブラリ: https://diagrams.mingrammer.com/ （バンドル画像は AWS が配布する
Architecture Icons と同系の承認済みセットです）
公式ダウンロード: https://aws.amazon.com/architecture/icons/

AWS Diagram MCP（awslabs.aws-diagram-mcp-server）との対応:
  - MCP の list_icons / get_diagram_examples / generate_diagram で使うクラス名と同一です。
  - MCP で PNG を出す場合は Graphviz の dot が PATH に必要です（generated-diagrams/README.md）。

依存:
  pip install diagrams
  Graphviz: https://graphviz.org/download/

実行（リポジトリルートから）:

  python Docs/diagrams-aiready-stacks-sketch.py

出力: このファイルと同じディレクトリに stacks_overview_sketch.png
論理構成のソースオブトゥルース: Docs/architecture-aiready-stacks.md（Mermaid）
"""

from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.compute import ECR, ECS, Lambda
from diagrams.aws.database import Aurora, Dynamodb
from diagrams.aws.integration import SNS, SQS
from diagrams.aws.management import Cloudwatch
from diagrams.aws.network import ELB
from diagrams.aws.security import SecretsManager


def main() -> None:
    out_dir = Path(__file__).resolve().parent
    graph_attr = {"pad": "0.4", "nodesep": "0.45", "ranksep": "0.55"}

    with Diagram(
        "AIReady stacks overview",
        filename=str(out_dir / "stacks_overview_sketch"),
        show=False,
        direction="TB",
        graph_attr=graph_attr,
    ):
        with Cluster("Connect"):
            conn_ddb = Dynamodb("FileMetadata\n+tables")
            conn_fn = Lambda("Connect\nLambdas")

        with Cluster("Governance"):
            gov_ddb = Dynamodb("AIReadyGov\nDDB")
            gov_fn = Lambda("analyzeExposure\nremediateFinding")

        with Cluster("Ontology Core"):
            ont_ddb = Dynamodb("Ontology\nDDB")
            fifo = SQS("EntityResolution\nFIFO")
            topic = SNS("Alerts")
            st = Lambda("schemaTransform")
            er = Lambda("entityResolver\nprofileUpdate\nlineageRecorder")

        with Cluster("Monitoring"):
            cw = Cloudwatch("Alarms\nDashboard")

        with Cluster("Aurora stack"):
            aurora = Aurora("Aurora PG")
            sec = SecretsManager("DB secret")

        with Cluster("Dashboard"):
            alb = ELB("ALB")
            ecs = ECS("Fargate\nAPI+Web")
            ecr = ECR("dashboard\napi+web")

        conn_fn >> Edge(label="write") >> conn_ddb
        conn_ddb >> Edge(label="DDB stream", style="bold") >> st
        conn_ddb >> Edge(label="DDB stream", style="bold") >> gov_fn

        st >> ont_ddb
        st >> gov_ddb
        fifo >> er
        er >> ont_ddb
        er >> topic

        cw >> Edge(label="Import SNS", style="dashed") >> topic

        alb >> ecs
        ecr - Edge(style="dotted", label="pull") - ecs

        ecs - Edge(style="dashed", label="optional_if_wired") - aurora
        aurora - sec


if __name__ == "__main__":
    main()

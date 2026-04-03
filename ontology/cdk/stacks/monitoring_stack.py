from aws_cdk import Duration, Stack
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_sns as sns
from constructs import Construct


class MonitoringStack(Stack):
    """CloudWatch alarms and dashboard for Ontology workloads."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        tenant_id: str,
        state_machine_arn: str | None = None,
        alert_topic_arn: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.tenant_id = tenant_id
        self.state_machine_arn = state_machine_arn
        self.alert_topic_arn = alert_topic_arn
        self._build_alarms_and_dashboard()

    def _build_alarms_and_dashboard(self) -> None:
        alarm_topic = None
        if self.alert_topic_arn:
            alarm_topic = sns.Topic.from_topic_arn(
                self,
                "OntologyAlertsTopic",
                self.alert_topic_arn,
            )

        schema_transform_errors = cloudwatch.Alarm(
            self,
            "SchemaTransformErrorsAlarm",
            alarm_name="Ontology-SchemaTransformErrors",
            metric=cloudwatch.Metric(
                namespace="AIReadyOntology",
                metric_name="SchemaTransformErrors",
                statistic="sum",
                period=Duration.minutes(5),
            ),
            threshold=10,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        entity_resolution_dlq_depth = cloudwatch.Alarm(
            self,
            "EntityResolutionDlqAlarm",
            alarm_name="Ontology-EntityResolutionDLQ",
            metric=cloudwatch.Metric(
                namespace="AWS/SQS",
                metric_name="ApproximateNumberOfMessagesVisible",
                dimensions_map={
                    "QueueName": "AIReadyOntology-EntityResolution-DLQ.fifo"
                },
                statistic="max",
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        pii_aggregation_alert = cloudwatch.Alarm(
            self,
            "PiiAggregationAlarm",
            alarm_name="Ontology-PIIAggregationAlert",
            metric=cloudwatch.Metric(
                namespace="AIReadyOntology",
                metric_name="PIIAggregationAlertFired",
                statistic="sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        stale_documents_alarm = cloudwatch.Alarm(
            self,
            "StaleDocumentsAlarm",
            alarm_name="Ontology-StaleDocuments",
            metric=cloudwatch.Metric(
                namespace="AIReadyOntology",
                metric_name="StaleDocuments",
                statistic="sum",
                period=Duration.days(1),
            ),
            threshold=100,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        governance_integration_inactive_alarm = cloudwatch.Alarm(
            self,
            "GovernanceIntegrationInactiveAlarm",
            alarm_name="Ontology-GovernanceIntegrationInactive",
            metric=cloudwatch.Metric(
                namespace="AIReadyOntology",
                metric_name="GovernanceIntegrationProcessed",
                statistic="sum",
                period=Duration.days(1),
            ),
            threshold=1,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.BREACHING,
        )

        if self.state_machine_arn:
            step_functions_failed = cloudwatch.Alarm(
                self,
                "BatchReconcilerStateMachineFailedAlarm",
                alarm_name="Ontology-BatchReconcilerFailure",
                metric=cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsFailed",
                    dimensions_map={"StateMachineArn": self.state_machine_arn},
                    statistic="sum",
                    period=Duration.minutes(5),
                ),
                threshold=1,
                evaluation_periods=3,
                datapoints_to_alarm=2,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )
            step_functions_failed_consecutive = cloudwatch.Alarm(
                self,
                "BatchReconcilerStateMachineFailedConsecutiveAlarm",
                alarm_name="Ontology-BatchReconcilerFailure-Consecutive",
                metric=cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionsFailed",
                    dimensions_map={"StateMachineArn": self.state_machine_arn},
                    statistic="sum",
                    period=Duration.minutes(5),
                ),
                threshold=1,
                evaluation_periods=3,
                datapoints_to_alarm=3,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )
            if alarm_topic:
                step_functions_failed.add_alarm_action(
                    cloudwatch_actions.SnsAction(alarm_topic)
                )
                step_functions_failed_consecutive.add_alarm_action(
                    cloudwatch_actions.SnsAction(alarm_topic)
                )

        if alarm_topic:
            for alarm in [
                schema_transform_errors,
                entity_resolution_dlq_depth,
                pii_aggregation_alert,
                stale_documents_alarm,
                governance_integration_inactive_alarm,
            ]:
                alarm.add_alarm_action(cloudwatch_actions.SnsAction(alarm_topic))

        dashboard = cloudwatch.Dashboard(
            self,
            "OntologyDashboard",
            dashboard_name="AIReadyOntology-Operations",
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Lambda Invocations",
                width=12,
                left=[
                    cloudwatch.Metric(
                        namespace="AWS/Lambda",
                        metric_name="Invocations",
                        dimensions_map={"FunctionName": "AIReadyOntology-schemaTransform"},
                        statistic="sum",
                        period=Duration.minutes(5),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Lambda",
                        metric_name="Invocations",
                        dimensions_map={"FunctionName": "AIReadyOntology-entityResolver"},
                        statistic="sum",
                        period=Duration.minutes(5),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Lambda",
                        metric_name="Invocations",
                        dimensions_map={"FunctionName": "AIReadyOntology-profileUpdate"},
                        statistic="sum",
                        period=Duration.minutes(5),
                    ),
                ],
            ),
            cloudwatch.GraphWidget(
                title="Lambda Errors",
                width=12,
                left=[
                    cloudwatch.Metric(
                        namespace="AWS/Lambda",
                        metric_name="Errors",
                        dimensions_map={"FunctionName": "AIReadyOntology-schemaTransform"},
                        statistic="sum",
                        period=Duration.minutes(5),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Lambda",
                        metric_name="Errors",
                        dimensions_map={"FunctionName": "AIReadyOntology-entityResolver"},
                        statistic="sum",
                        period=Duration.minutes(5),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Lambda",
                        metric_name="Errors",
                        dimensions_map={"FunctionName": "AIReadyOntology-profileUpdate"},
                        statistic="sum",
                        period=Duration.minutes(5),
                    ),
                ],
            ),
            cloudwatch.GraphWidget(
                title="Dual-run Compare (Gov vs Ontology Shadow)",
                width=12,
                left=[
                    cloudwatch.Metric(
                        namespace="AIReadyGovernance",
                        metric_name="AIReadyGov.EntityCandidatesEnqueued",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Gov Entity Candidates Enqueued",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyGovernance",
                        metric_name="AIReadyGov.EntityCandidatesSkippedByFlag",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Gov Entity Candidates SkippedByFlag",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="ShadowAnalysisEligibleDocuments",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Ontology Shadow Eligible Documents",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="ShadowAnalysisCandidateMentions",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Ontology Shadow Candidate Mentions",
                    ),
                ],
            ),
            cloudwatch.GraphWidget(
                title="Shadow Skip Breakdown",
                width=12,
                left=[
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="ShadowAnalysisSkippedOrphan",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Skipped Orphan",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="ShadowAnalysisSkippedStale",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Skipped Stale",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="ShadowAnalysisSkippedNonCanonical",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Skipped Non Canonical",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="ShadowAnalysisSkippedIneligible",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Skipped Ineligible",
                    ),
                ],
            ),
            cloudwatch.GraphWidget(
                title="Write Switch Progress",
                width=12,
                left=[
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="WriteSwitchEnqueuedDocuments",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Ontology WriteSwitch Enqueued Documents",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="WriteSwitchEnqueuedCandidates",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Ontology WriteSwitch Candidate Count",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="WriteSwitchSkippedByRollout",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Ontology WriteSwitch SkippedByRollout",
                    ),
                    cloudwatch.Metric(
                        namespace="AIReadyOntology",
                        metric_name="WriteSwitchSkippedByFlag",
                        statistic="sum",
                        period=Duration.minutes(5),
                        label="Ontology WriteSwitch SkippedByFlag",
                    ),
                ],
            ),
            cloudwatch.LogQueryWidget(
                title="Logs Insights: schemaTransform errors",
                width=24,
                view=cloudwatch.LogQueryVisualizationType.TABLE,
                log_group_names=["/aws/lambda/AIReadyOntology-schemaTransform"],
                query_lines=[
                    "fields @timestamp, level, message, error",
                    "filter level = 'ERROR'",
                    "sort @timestamp desc",
                    "limit 100",
                ],
            ),
            cloudwatch.LogQueryWidget(
                title="Logs Insights: entityResolver errors",
                width=24,
                view=cloudwatch.LogQueryVisualizationType.TABLE,
                log_group_names=["/aws/lambda/AIReadyOntology-entityResolver"],
                query_lines=[
                    "fields @timestamp, level, message, error",
                    "filter level = 'ERROR'",
                    "sort @timestamp desc",
                    "limit 100",
                ],
            ),
        )

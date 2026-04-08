"""
This module contains graph, Connect, Governance, Ontology, and dashboard routes for the GraphSuite API.
"""

import asyncio
from typing import Optional
import traceback
from time import perf_counter
from fastapi import APIRouter, Body, Depends, Query, HTTPException, Request, Response, status

import logging

logger = logging.getLogger(__name__)

from utils_api import get_combined_auth_dependency, get_tenant_context_dependency
from tenant_context import TenantContext
from schemas.graph_schema import EntityUpdateRequest, RelationUpdateRequest
from schemas.connect_schema import (
    ConnectOnboardingDefaultsResponse,
    ConnectOnboardingRequest,
    ConnectOnboardingResponse,
    ConnectAuditResponse,
    ConnectEventsResponse,
    ConnectJobsResponse,
    ConnectOverviewDto,
    ConnectSiteDiscoveryRequest,
    ConnectSiteDiscoveryResponse,
    ConnectSiteOptionsRequest,
    ConnectSiteOptionsResponse,
    ConnectTeamChannelOptionsRequest,
    ConnectTeamChannelOptionsResponse,
    ConnectScopesResponse,
    ConnectSubscriptionDeleteResponse,
    ConnectSubscriptionsResponse,
    ConnectSyncCheckResponse,
)
from schemas.dashboard_audit_schema import (
    AuditExportAcceptedResponse,
    AuditExportRequest,
    AuditExportStatusResponse,
    AuditRecordsResponse,
    DashboardReadinessResponse,
    DashboardReadinessTrendResponse,
    DashboardRecommendedActionsResponse,
)
from services.ontology_graph_repository import (
    get_ontology_graph,
    get_ontology_graph_by_item as get_ontology_item_graph_from_repository,
    get_resolved_ontology_graph_db_path,
    get_ontology_labels,
    get_ontology_entity_candidates,
    resolve_ontology_entity_candidate_existing,
    register_ontology_entity_candidate_new,
    seed_ontology_graph_sample_data,
)
from services.governance_repository import list_governance_findings
from services.audit_log import build_audit_log
from services.dashboard_audit_service import (
    create_audit_export_job,
    get_audit_export_download,
    get_audit_export_job_status,
    get_dashboard_readiness,
    get_dashboard_readiness_trend,
    list_dashboard_recommended_actions,
    list_audit_records,
)
from services.repositories.audit_writer_repository import CommonAuditRepository
from services.governance_api_service import (
    GovernanceRemediationProxyError,
    approve_governance_remediation,
    execute_governance_remediation,
    get_governance_finding_remediation,
    get_governance_overview,
    list_governance_suppressions,
    log_governance_remediation_route_debug,
    propose_governance_remediation,
    trigger_governance_daily_scan,
    list_governance_policies,
    create_governance_policy,
    update_governance_policy,
    simulate_governance_policy,
    list_governance_scan_jobs,
    list_governance_audit_logs,
    mark_governance_finding_completed,
    register_governance_finding_exception,
    rollback_governance_remediation,
)
from services.connect_service import (
    get_connect_onboarding_defaults,
    create_connect_onboarding,
    delete_connect_subscription,
    get_connect_overview,
    collect_file_metadata_item_ids_for_drives,
    get_active_connect_drive_ids,
    list_connect_audit,
    list_connect_events,
    list_connect_jobs,
    list_connect_scopes,
    list_connect_subscriptions,
    list_connect_site_options,
    list_connect_team_channel_options,
    resolve_connect_site_discovery,
    trigger_connect_sync_check,
)
from services.ontology_api_service import (
    OntologyDataAccessError,
    get_ontology_overview,
    refresh_ontology_graph_projection,
    list_ontology_unified_metadata,
    list_ontology_entity_master,
    list_ontology_audit_logs,
    get_ontology_user_settings,
    update_ontology_user_projection_preset,
    update_ontology_user_settings,
    ai_fill_unified_metadata_profile,
    update_unified_metadata_profile,
)

router = APIRouter(tags=["graph"])
_common_audit_repository = CommonAuditRepository()
_CACHE_HIT_FIELD = "_cache_hit"


def _ontology_projection_diagnostics(tenant_id: str) -> dict[str, object]:
    """SQLite path and ops hints for projection vs UnifiedMetadata mismatch debugging."""
    out: dict[str, object] = {
        "projection_storage": "sqlite_local",
        "multi_instance_note": (
            "SQLite is local to each API host; multiple instances may see an empty graph until each "
            "runs projection refresh or ONTOLOGY_GRAPH_DB_PATH points to shared attached storage."
        ),
    }
    try:
        out["ontology_graph_sqlite_path"] = get_resolved_ontology_graph_db_path(tenant_id)
    except Exception as exc:
        out["ontology_graph_sqlite_path"] = None
        out["ontology_graph_sqlite_path_error"] = str(exc)
    return out


def _resolve_operator(
    tenant_context: TenantContext, operator: str | None, *, action_name: str
) -> str:
    requested_operator = str(operator or "").strip()
    if not requested_operator:
        return tenant_context.username
    if requested_operator != tenant_context.username and not tenant_context.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Operator override is forbidden for action '{action_name}' "
                "without admin role."
            ),
        )
    return requested_operator


def _log_audit(
    request: Request,
    tenant_context: TenantContext,
    event: str,
    operator: str,
    attributes: dict | None = None,
) -> None:
    correlation_id = getattr(request.state, "correlation_id", "")
    normalized_attributes = attributes or {}
    target = (
        str(
            normalized_attributes.get("target")
            or normalized_attributes.get("plan_id")
            or normalized_attributes.get("job_id")
            or normalized_attributes.get("candidate_id")
            or ""
        )
        .strip()
    )
    event_tokens = str(event or "").split(".")
    domain = event_tokens[0] if event_tokens else "unknown"
    action = ".".join(event_tokens[1:]) if len(event_tokens) > 1 else event
    logger.info(
        build_audit_log(
            event=event,
            tenant_id=tenant_context.tenant_id,
            correlation_id=correlation_id,
            operator=operator,
            attributes=normalized_attributes,
        )
    )
    try:
        _common_audit_repository.append(
            tenant_id=tenant_context.tenant_id,
            domain=domain,
            actor=operator,
            action=str(action or event),
            target=target,
            correlation_id=correlation_id,
            metadata=normalized_attributes,
        )
    except Exception:
        # Audit persistence must not block primary operations.
        pass


def _log_connect_observability(
    request: Request | None,
    tenant_context: TenantContext,
    *,
    route: str,
    status_code: int,
    started_at: float,
    extra: dict | None = None,
) -> None:
    correlation_id = getattr(request.state, "correlation_id", "") if request else ""
    latency_ms = round((perf_counter() - started_at) * 1000, 2)
    payload = {
        "event": "connect.api.observability",
        "tenant_id": tenant_context.tenant_id,
        "correlation_id": correlation_id,
        "route": route,
        "status_code": status_code,
        "latency_ms": latency_ms,
    }
    if extra:
        payload["extra"] = extra
    logger.info(payload)


def _set_connect_cache_header(response: Response | None, payload: dict) -> dict:
    cache_status = str(payload.pop(_CACHE_HIT_FIELD, "MISS"))
    if response is not None:
        response.headers["X-Cache"] = cache_status
    return payload


def _reject_tenant_override(request: Request | None, payload: dict | None = None) -> None:
    if request and "tenant_id" in request.query_params:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="tenant_id must not be provided in request query.",
        )
    if isinstance(payload, dict) and "tenant_id" in payload:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="tenant_id must not be provided in request body.",
        )


def _ontology_http_exception(
    *,
    status_code: int,
    error_code: str,
    detail: str,
    request: Request | None,
) -> HTTPException:
    correlation_id = getattr(request.state, "correlation_id", "") if request else ""
    return HTTPException(
        status_code=status_code,
        detail={
            "error_code": error_code,
            "detail": detail,
            "status": status_code,
            "correlation_id": correlation_id,
        },
    )


def _is_not_found_error(message: str) -> bool:
    normalized = message.lower()
    return "not found" in normalized


def _ontology_error_code_from_value_error(
    message: str,
    *,
    default_400: str = "ontology.invalid_request",
    default_404: str = "ontology.item_not_found",
) -> tuple[int, str]:
    if _is_not_found_error(message):
        return status.HTTP_404_NOT_FOUND, default_404
    return status.HTTP_400_BAD_REQUEST, default_400


_EMBEDDED_RAG_DISABLED_DETAIL = (
    "Default (embedded RAG) graph endpoints are not available in this build. "
    "Use Ontology graph APIs (e.g. source=ontology on /graphs where supported)."
)


def _embedded_rag_graph_disabled() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail=_EMBEDDED_RAG_DISABLED_DETAIL,
    )


def create_graph_routes(api_key: Optional[str] = None):
    combined_auth = get_combined_auth_dependency(api_key)

    @router.get("/graph/label/list", dependencies=[Depends(combined_auth)])
    async def get_graph_labels(
        source: str = Query("default", description="Graph data source: default | ontology"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Get all graph labels

        Returns:
            List[str]: List of graph labels
        """
        try:
            if source == "ontology":
                _log_audit(
                    request,
                    tenant_context,
                    "ontology.graph_labels.read",
                    tenant_context.username,
                    {"source": source},
                )
                return get_ontology_labels(tenant_id=tenant_context.tenant_id)
            raise _embedded_rag_graph_disabled()
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting graph labels: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500, detail=f"Error getting graph labels: {str(e)}"
            )

    @router.get("/graph/label/popular", dependencies=[Depends(combined_auth)])
    async def get_popular_labels(
        limit: int = Query(
            300, description="Maximum number of popular labels to return", ge=1, le=1000
        ),
    ):
        """
        Get popular labels by node degree (most connected entities)

        Args:
            limit (int): Maximum number of labels to return (default: 300, max: 1000)

        Returns:
            List[str]: List of popular labels sorted by degree (highest first)
        """
        raise _embedded_rag_graph_disabled()

    @router.get("/graph/label/search", dependencies=[Depends(combined_auth)])
    async def search_labels(
        q: str = Query(..., description="Search query string"),
        limit: int = Query(
            50, description="Maximum number of search results to return", ge=1, le=100
        ),
    ):
        """
        Search labels with fuzzy matching

        Args:
            q (str): Search query string
            limit (int): Maximum number of results to return (default: 50, max: 100)

        Returns:
            List[str]: List of matching labels sorted by relevance
        """
        raise _embedded_rag_graph_disabled()

    @router.get("/graphs", dependencies=[Depends(combined_auth)])
    async def get_knowledge_graph(
        label: str = Query(..., description="Label to get knowledge graph for"),
        max_depth: int = Query(3, description="Maximum depth of graph", ge=1),
        max_nodes: int = Query(1000, description="Maximum nodes to return", ge=1),
        source: str = Query("default", description="Graph data source: default | ontology"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Retrieve a connected subgraph of nodes where the label includes the specified label.
        When reducing the number of nodes, the prioritization criteria are as follows:
            1. Hops(path) to the staring node take precedence
            2. Followed by the degree of the nodes

        Args:
            label (str): Label of the starting node
            max_depth (int, optional): Maximum depth of the subgraph,Defaults to 3
            max_nodes: Maxiumu nodes to return

        Returns:
            Dict[str, List[str]]: Knowledge graph for label
        """
        try:
            if source == "ontology":
                _log_audit(
                    request,
                    tenant_context,
                    "ontology.graph.read",
                    tenant_context.username,
                    {"label": label},
                )
                graph_payload = get_ontology_graph(
                    tenant_id=tenant_context.tenant_id,
                    label=label,
                    max_depth=max_depth,
                    max_nodes=max_nodes,
                )
                if not isinstance(graph_payload, dict):
                    return graph_payload
                nodes = graph_payload.get("nodes")
                normalized_label = str(label or "").strip()
                if not nodes:
                    unified_access_error: str | None = None
                    try:
                        unified = list_ontology_unified_metadata(
                            tenant_id=tenant_context.tenant_id,
                            limit=1,
                            offset=0,
                        )
                        total_count = int(
                            (
                                unified.get("pagination", {})
                                if isinstance(unified, dict)
                                else {}
                            ).get("total_count", 0)
                        )
                    except OntologyDataAccessError as data_access_error:
                        total_count = 0
                        unified_access_error = str(data_access_error)
                    except Exception as unified_query_error:
                        total_count = 0
                        unified_access_error = str(unified_query_error) or "Failed to query UnifiedMetadata."
                    if unified_access_error:
                        graph_payload["projection_status"] = {
                            "state": "projection_refresh_failed",
                            "message": (
                                "UnifiedMetadata could not be queried for ontology projection. "
                                "Check API logs and DynamoDB tenant/table settings."
                            ),
                            "unified_total_count": total_count,
                            "error": unified_access_error,
                        }
                        return graph_payload
                    if total_count > 0:
                        if normalized_label in {"", "*"}:
                            try:
                                refresh_result = refresh_ontology_graph_projection(
                                    tenant_id=tenant_context.tenant_id,
                                    clear_existing=True,
                                    max_documents=2000,
                                    preset=None,
                                )
                                graph_payload = get_ontology_graph(
                                    tenant_id=tenant_context.tenant_id,
                                    label=label,
                                    max_depth=max_depth,
                                    max_nodes=max_nodes,
                                )
                                if not isinstance(graph_payload, dict):
                                    graph_payload = {"nodes": [], "edges": [], "is_truncated": False}
                                refreshed_nodes = (
                                    graph_payload.get("nodes")
                                    if isinstance(graph_payload, dict)
                                    else []
                                )
                                if refreshed_nodes:
                                    graph_payload["projection_status"] = {
                                        "state": "projection_auto_refreshed",
                                        "message": "Ontology graph projection was automatically refreshed.",
                                        "unified_total_count": total_count,
                                        "projected_documents": int(
                                            (refresh_result or {}).get("projected_documents", 0)
                                        ),
                                    }
                                else:
                                    diag = _ontology_projection_diagnostics(tenant_context.tenant_id)
                                    logger.warning(
                                        "Ontology graph still empty after auto projection refresh: "
                                        "tenant_id=%s unified_total_count=%s label=%r sqlite_path=%s",
                                        tenant_context.tenant_id,
                                        total_count,
                                        label,
                                        diag.get("ontology_graph_sqlite_path"),
                                    )
                                    graph_payload["projection_status"] = {
                                        "state": "projection_stale_or_empty",
                                        "message": (
                                            "UnifiedMetadata has data, but ontology graph projection is empty. "
                                            "Run /ontology/graph/projection/refresh."
                                        ),
                                        "unified_total_count": total_count,
                                        **diag,
                                    }
                            except Exception as refresh_error:
                                diag = _ontology_projection_diagnostics(tenant_context.tenant_id)
                                logger.warning(
                                    "Ontology graph auto projection refresh failed: %s sqlite_path=%s",
                                    str(refresh_error),
                                    diag.get("ontology_graph_sqlite_path"),
                                    exc_info=True,
                                )
                                graph_payload["projection_status"] = {
                                    "state": "projection_refresh_failed",
                                    "message": (
                                        "UnifiedMetadata has data, but automatic projection refresh failed. "
                                        "Run /ontology/graph/projection/refresh."
                                    ),
                                    "unified_total_count": total_count,
                                    "error": str(refresh_error),
                                    **diag,
                                }
                        else:
                            diag = _ontology_projection_diagnostics(tenant_context.tenant_id)
                            logger.warning(
                                "Ontology graph projection empty (non-wildcard label; no auto-refresh): "
                                "tenant_id=%s unified_total_count=%s label=%r sqlite_path=%s",
                                tenant_context.tenant_id,
                                total_count,
                                label,
                                diag.get("ontology_graph_sqlite_path"),
                            )
                            graph_payload["projection_status"] = {
                                "state": "projection_stale_or_empty",
                                "message": (
                                    "UnifiedMetadata has data, but ontology graph projection is empty. "
                                    "Run /ontology/graph/projection/refresh."
                                ),
                                "unified_total_count": total_count,
                                **diag,
                            }
                return graph_payload
            raise _embedded_rag_graph_disabled()
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting knowledge graph for label '{label}': {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500, detail=f"Error getting knowledge graph: {str(e)}"
            )

    @router.post("/ontology/seed", dependencies=[Depends(combined_auth)])
    async def seed_ontology_graph(
        force: bool = Query(False, description="Reseed ontology graph data when true"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Seed ontology-focused graph data into the dedicated ontology SQLite database.
        """
        try:
            _reject_tenant_override(request)
            if not tenant_context.is_admin:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Ontology seed is allowed for admin role only.",
                )
            _log_audit(
                request,
                tenant_context,
                "ontology.seed",
                tenant_context.username,
                {"force": force},
            )
            return seed_ontology_graph_sample_data(
                tenant_id=tenant_context.tenant_id, force=force
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error seeding ontology graph data: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error seeding ontology graph: {str(e)}")

    @router.get("/ontology/graph/by-item", dependencies=[Depends(combined_auth)])
    async def get_ontology_graph_for_item(
        item_id: str = Query(..., description="Document item id (e.g., DOC-001)"),
        file_name: str = Query("", description="Optional file name/title used as fallback matcher"),
        max_depth: int = Query(2, description="Maximum depth of item-focused graph", ge=1),
        max_nodes: int = Query(80, description="Maximum nodes to return", ge=1),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Retrieve an item-focused subgraph from the ontology graph projection store.
        """
        try:
            _reject_tenant_override(request)
            graph = get_ontology_item_graph_from_repository(
                tenant_id=tenant_context.tenant_id,
                item_id=item_id,
                file_name=file_name,
                max_depth=max_depth,
                max_nodes=max_nodes,
            )
            _log_audit(
                request,
                tenant_context,
                "ontology.item_graph.read",
                tenant_context.username,
                {"item_id": item_id, "max_depth": max_depth, "max_nodes": max_nodes},
            )
            if "is_truncated" not in graph:
                graph["is_truncated"] = False
            return graph
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting ontology item graph for '{item_id}': {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting ontology item graph: {str(e)}")

    @router.post("/ontology/graph/projection/refresh", dependencies=[Depends(combined_auth)])
    async def refresh_ontology_projection(
        clear_existing: bool = Query(True, description="Delete existing ontology graph projection before rebuild"),
        max_documents: int = Query(2000, description="Maximum UnifiedMetadata rows to project", ge=1, le=10000),
        preset: str | None = Query(
            None,
            description="Projection preset override: strict | standard | relaxed",
        ),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Rebuild ontology graph projection from AWS UnifiedMetadata.
        """
        try:
            _reject_tenant_override(request)
            result = refresh_ontology_graph_projection(
                tenant_id=tenant_context.tenant_id,
                clear_existing=clear_existing,
                max_documents=max_documents,
                preset=preset,
            )
            _log_audit(
                request,
                tenant_context,
                "ontology.graph_projection.refresh",
                tenant_context.username,
                {
                    "clear_existing": clear_existing,
                    "max_documents": max_documents,
                    "source_documents": result.get("source_documents", 0),
                    "projected_documents": result.get("projected_documents", 0),
                    "contained_in_links": result.get("contained_in_links", 0),
                    "mentions_links": result.get("mentions_links", 0),
                    "similarity_links": result.get("similarity_links", 0),
                    "skipped_similarity_docs": result.get("skipped_similarity_docs", 0),
                    "text_fallback_vectors": result.get("text_fallback_vectors", 0),
                    "auto_promoted_entities": result.get("auto_promoted_entities", 0),
                    "projection_preset": result.get("projection_preset", "standard"),
                    "projection_preset_source": result.get("projection_preset_source", "default"),
                },
            )
            return result
        except HTTPException:
            raise
        except ValueError as e:
            raise _ontology_http_exception(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code="ontology.invalid_request",
                detail=str(e),
                request=request,
            )
        except Exception as e:
            logger.error(f"Error refreshing ontology graph projection: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error refreshing ontology graph projection: {str(e)}",
                request=request,
            )

    @router.get("/ontology/user-settings", dependencies=[Depends(combined_auth)])
    async def get_ontology_user_settings_route(
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            return get_ontology_user_settings(
                tenant_id=tenant_context.tenant_id,
                username=tenant_context.username,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting ontology user settings: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error getting ontology user settings: {str(e)}",
                request=request,
            )

    @router.put("/ontology/user-settings/projection-preset", dependencies=[Depends(combined_auth)])
    async def update_ontology_user_projection_preset_route(
        preset: str = Query(..., description="Projection preset: strict | standard | relaxed"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            result = update_ontology_user_projection_preset(
                tenant_id=tenant_context.tenant_id,
                username=tenant_context.username,
                projection_preset=preset,
            )
            _log_audit(
                request,
                tenant_context,
                "ontology.user_settings.projection_preset.update",
                tenant_context.username,
                {"projection_preset": result.get("projection_preset", "standard")},
            )
            return result
        except HTTPException:
            raise
        except ValueError as e:
            raise _ontology_http_exception(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code="ontology.projection_preset_invalid",
                detail=str(e),
                request=request,
            )
        except Exception as e:
            logger.error(f"Error updating ontology projection preset setting: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error updating ontology projection preset setting: {str(e)}",
                request=request,
            )

    @router.put("/ontology/user-settings", dependencies=[Depends(combined_auth)])
    async def update_ontology_user_settings_route(
        projection_preset: str | None = Query(None, description="Projection preset: strict | standard | relaxed"),
        max_documents: int | None = Query(None, description="Default max documents for refresh", ge=1, le=10000),
        auto_refresh: bool | None = Query(None, description="Enable auto refresh on overview load"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            if projection_preset is None and max_documents is None and auto_refresh is None:
                raise _ontology_http_exception(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    error_code="ontology.invalid_request",
                    detail="At least one parameter is required: projection_preset, max_documents, auto_refresh.",
                    request=request,
                )
            result = update_ontology_user_settings(
                tenant_id=tenant_context.tenant_id,
                username=tenant_context.username,
                projection_preset=projection_preset,
                max_documents=max_documents,
                auto_refresh=auto_refresh,
            )
            _log_audit(
                request,
                tenant_context,
                "ontology.user_settings.update",
                tenant_context.username,
                {
                    "projection_preset": result.get("projection_preset", "standard"),
                    "max_documents": result.get("max_documents", 2000),
                    "auto_refresh": result.get("auto_refresh", True),
                },
            )
            return result
        except HTTPException:
            raise
        except ValueError as e:
            raise _ontology_http_exception(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code="ontology.invalid_request",
                detail=str(e),
                request=request,
            )
        except Exception as e:
            logger.error(f"Error updating ontology user settings: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error updating ontology user settings: {str(e)}",
                request=request,
            )

    @router.get("/ontology/entity-candidates", dependencies=[Depends(combined_auth)])
    async def get_ontology_candidates(
        limit: int = Query(100, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        status: str = Query("pending", description="Candidate status filter"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Get extracted word candidates with dictionary-based similarity suggestions.
        """
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "ontology.candidates.read",
                tenant_context.username,
                {"status": status},
            )
            return get_ontology_entity_candidates(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
                status=status,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting ontology candidates: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error getting ontology candidates: {str(e)}",
                request=request,
            )

    @router.get(
        "/dashboard/readiness",
        response_model=DashboardReadinessResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_dashboard_readiness_route(
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            return get_dashboard_readiness(tenant_id=tenant_context.tenant_id)
        except Exception as e:
            logger.error(f"Error getting dashboard readiness: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting dashboard readiness: {str(e)}")

    @router.get(
        "/dashboard/readiness/trend",
        response_model=DashboardReadinessTrendResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_dashboard_readiness_trend_route(
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            return get_dashboard_readiness_trend(tenant_id=tenant_context.tenant_id)
        except Exception as e:
            logger.error(f"Error getting dashboard readiness trend: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting dashboard readiness trend: {str(e)}")

    @router.get(
        "/dashboard/recommended-actions",
        response_model=DashboardRecommendedActionsResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_dashboard_recommended_actions_route(
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            return list_dashboard_recommended_actions(tenant_id=tenant_context.tenant_id)
        except Exception as e:
            logger.error(f"Error getting dashboard recommended actions: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting dashboard recommended actions: {str(e)}")

    @router.get(
        "/audit/records",
        response_model=AuditRecordsResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_audit_records_route(
        domain: str = Query("all", description="all | connect | governance | ontology"),
        q: str = Query("", description="free text search"),
        limit: int = Query(100, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            return list_audit_records(
                tenant_id=tenant_context.tenant_id,
                domain=domain,
                q=q,
                limit=limit,
                offset=offset,
            )
        except Exception as e:
            logger.error(f"Error getting audit records: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting audit records: {str(e)}")

    @router.post(
        "/audit/exports",
        response_model=AuditExportAcceptedResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def create_audit_export_route(
        payload: AuditExportRequest,
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            return create_audit_export_job(
                tenant_id=tenant_context.tenant_id,
                domain=payload.domain,
                q=payload.q,
                export_format=payload.format,
            )
        except Exception as e:
            logger.error(f"Error creating audit export: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error creating audit export: {str(e)}")

    @router.get(
        "/audit/exports/{job_id}",
        response_model=AuditExportStatusResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_audit_export_status_route(
        job_id: str,
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            return get_audit_export_job_status(
                tenant_id=tenant_context.tenant_id,
                job_id=job_id,
            )
        except Exception as e:
            logger.error(f"Error getting audit export status: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting audit export status: {str(e)}")

    @router.get(
        "/audit/exports/{job_id}/download",
        dependencies=[Depends(combined_auth)],
    )
    async def download_audit_export_route(
        job_id: str,
        token: str = Query(..., description="One-time download token"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            download = get_audit_export_download(
                tenant_id=tenant_context.tenant_id,
                job_id=job_id,
                token=token,
            )
            headers = {
                "Content-Disposition": f"attachment; filename={download['file_name']}",
                "Cache-Control": "no-store",
            }
            return Response(
                content=download["payload_bytes"],
                media_type=download["content_type"],
                headers=headers,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as e:
            logger.error(f"Error downloading audit export: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error downloading audit export: {str(e)}")

    @router.get(
        "/connect/overview",
        response_model=ConnectOverviewDto,
        dependencies=[Depends(combined_auth)],
    )
    async def get_connect_overview_route(
        request: Request,
        response: Response,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.overview.read",
                tenant_context.username,
            )
            payload = get_connect_overview(tenant_id=tenant_context.tenant_id)
            return _set_connect_cache_header(response, payload)
        except Exception as e:
            status_code = 500
            logger.error(f"Error getting connect overview: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting connect overview: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/overview",
                status_code=status_code,
                started_at=started_at,
            )

    @router.get(
        "/connect/subscriptions",
        response_model=ConnectSubscriptionsResponse,
        dependencies=[Depends(combined_auth)],
        summary="Connect 購読一覧",
        description=(
            "Connections の接続行を返します。status が initializing のときは Microsoft Graph の "
            "subscription_id が DynamoDB / SSM に未保存（init_subscription 未完了・失敗など）の可能性があります。"
        ),
    )
    async def get_connect_subscriptions_route(
        request: Request,
        response: Response,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.subscriptions.read",
                tenant_context.username,
            )
            payload = list_connect_subscriptions(tenant_id=tenant_context.tenant_id)
            return _set_connect_cache_header(response, payload)
        except Exception as e:
            status_code = 500
            logger.error(f"Error getting connect subscriptions: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting connect subscriptions: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/subscriptions",
                status_code=status_code,
                started_at=started_at,
            )

    @router.delete(
        "/connect/subscriptions/{subscription_id}",
        response_model=ConnectSubscriptionDeleteResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def delete_connect_subscription_route(
        subscription_id: str,
        connection_id: str = Query("", description="Optional connection id"),
        delete_mode: str = Query("safe", description="Delete mode: safe or force"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.subscriptions.delete",
                tenant_context.username,
                {"subscription_id": subscription_id, "connection_id": connection_id, "delete_mode": delete_mode},
            )
            return delete_connect_subscription(
                tenant_id=tenant_context.tenant_id,
                subscription_id=subscription_id,
                connection_id=connection_id,
                delete_mode=delete_mode,
            )
        except ValueError as e:
            status_code = 400
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            status_code = 500
            logger.error(f"Error deleting connect subscription: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error deleting connect subscription: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/subscriptions/{subscription_id}",
                status_code=status_code,
                started_at=started_at,
                extra={"subscription_id": subscription_id, "connection_id": connection_id, "delete_mode": delete_mode},
            )

    @router.get(
        "/connect/scopes",
        response_model=ConnectScopesResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_connect_scopes_route(
        subscription_id: str = Query("", description="Optional subscription id"),
        request: Request = None,
        response: Response = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.scopes.read",
                tenant_context.username,
                {"subscription_id": subscription_id},
            )
            payload = list_connect_scopes(
                tenant_id=tenant_context.tenant_id,
                subscription_id=subscription_id or None,
            )
            return _set_connect_cache_header(response, payload)
        except Exception as e:
            status_code = 500
            logger.error(f"Error getting connect scopes: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting connect scopes: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/scopes",
                status_code=status_code,
                started_at=started_at,
                extra={"subscription_id": subscription_id},
            )

    @router.get(
        "/connect/events",
        response_model=ConnectEventsResponse,
        dependencies=[Depends(combined_auth)],
        summary="Connect 受信イベント一覧",
        description=(
            "ドライブ接続では DynamoDB FileMetadata の取り込み済み行を、メッセージ接続では MessageMetadata を "
            "イベント表示用に返します。メタデータを削除した直後や同期が止まっていると 0 件になります。"
        ),
    )
    async def get_connect_events_route(
        scope_id: str = Query("", description="Optional scope id"),
        status_filter: str = Query("", alias="status", description="Optional event status"),
        limit: int = Query(100, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        response: Response = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.events.read",
                tenant_context.username,
                {"scope_id": scope_id, "status": status_filter},
            )
            payload = list_connect_events(
                tenant_id=tenant_context.tenant_id,
                scope_id=scope_id or None,
                status=status_filter or None,
                limit=limit,
                offset=offset,
            )
            return _set_connect_cache_header(response, payload)
        except Exception as e:
            status_code = 500
            logger.error(f"Error getting connect events: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting connect events: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/events",
                status_code=status_code,
                started_at=started_at,
                extra={"scope_id": scope_id, "status": status_filter, "limit": limit, "offset": offset},
            )

    @router.get(
        "/connect/jobs",
        response_model=ConnectJobsResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_connect_jobs_route(
        event_id: str = Query("", description="Optional event id"),
        status_filter: str = Query("", alias="status", description="Optional job status"),
        limit: int = Query(100, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        response: Response = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.jobs.read",
                tenant_context.username,
                {"event_id": event_id, "status": status_filter},
            )
            payload = list_connect_jobs(
                tenant_id=tenant_context.tenant_id,
                event_id=event_id or None,
                status=status_filter or None,
                limit=limit,
                offset=offset,
            )
            return _set_connect_cache_header(response, payload)
        except Exception as e:
            status_code = 500
            logger.error(f"Error getting connect jobs: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting connect jobs: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/jobs",
                status_code=status_code,
                started_at=started_at,
                extra={"event_id": event_id, "status": status_filter, "limit": limit, "offset": offset},
            )

    @router.get(
        "/connect/audit",
        response_model=ConnectAuditResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_connect_audit_route(
        q: str = Query("", description="Free text query"),
        limit: int = Query(100, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        response: Response = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.audit.read",
                tenant_context.username,
                {"q": q},
            )
            payload = list_connect_audit(
                tenant_id=tenant_context.tenant_id,
                query=q or None,
                limit=limit,
                offset=offset,
            )
            return _set_connect_cache_header(response, payload)
        except Exception as e:
            status_code = 500
            logger.error(f"Error getting connect audit: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting connect audit: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/audit",
                status_code=status_code,
                started_at=started_at,
                extra={"q": q, "limit": limit, "offset": offset},
            )

    @router.post(
        "/connect/sync/check",
        response_model=ConnectSyncCheckResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def post_connect_sync_check_route(
        requested_by: str | None = Query(None, description="Sync trigger operator id"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            operator = _resolve_operator(
                tenant_context, requested_by, action_name="connect_sync_check"
            )
            correlation_id = getattr(request.state, "correlation_id", "")
            _log_audit(
                request,
                tenant_context,
                "connect.sync.check",
                operator,
                {"correlation_id": correlation_id},
            )
            return trigger_connect_sync_check(
                tenant_id=tenant_context.tenant_id,
                requested_by=operator,
                correlation_id=correlation_id,
            )
        except HTTPException:
            raise
        except Exception as e:
            status_code = 500
            logger.error(f"Error triggering connect sync check: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error triggering connect sync check: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/sync/check",
                status_code=status_code,
                started_at=started_at,
            )

    @router.get(
        "/connect/onboarding/defaults",
        response_model=ConnectOnboardingDefaultsResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def get_connect_onboarding_defaults_route(
        request: Request,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.onboarding.defaults.read",
                tenant_context.username,
            )
            return get_connect_onboarding_defaults(tenant_id=tenant_context.tenant_id)
        except Exception as e:
            status_code = 500
            logger.error(f"Error getting connect onboarding defaults: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"Error getting connect onboarding defaults: {str(e)}",
            )
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/onboarding/defaults",
                status_code=status_code,
                started_at=started_at,
            )

    @router.post(
        "/connect/onboarding/site-discovery",
        response_model=ConnectSiteDiscoveryResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def post_connect_onboarding_site_discovery_route(
        payload: ConnectSiteDiscoveryRequest,
        request: Request,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.onboarding.site_discovery.read",
                tenant_context.username,
            )
            return resolve_connect_site_discovery(
                tenant_id=tenant_context.tenant_id,
                azure_tenant_id=payload.azure_tenant_id,
                client_id=payload.client_id,
                client_secret=payload.client_secret,
                site_url=payload.site_url,
                site_id=payload.site_id,
            )
        except ValueError as e:
            status_code = 400
            raise HTTPException(status_code=400, detail=str(e))
        except RuntimeError as e:
            status_code = 502
            raise HTTPException(status_code=502, detail=str(e))
        except Exception as e:
            status_code = 500
            logger.error(f"Error resolving connect site discovery: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"Error resolving connect site discovery: {str(e)}",
            )
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/onboarding/site-discovery",
                status_code=status_code,
                started_at=started_at,
            )

    @router.post(
        "/connect/onboarding/site-options",
        response_model=ConnectSiteOptionsResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def post_connect_onboarding_site_options_route(
        payload: ConnectSiteOptionsRequest,
        request: Request,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.onboarding.site_options.read",
                tenant_context.username,
            )
            return list_connect_site_options(
                tenant_id=tenant_context.tenant_id,
                azure_tenant_id=payload.azure_tenant_id,
                client_id=payload.client_id,
                client_secret=payload.client_secret,
                source_type=payload.source_type,
                query=payload.query,
            )
        except ValueError as e:
            status_code = 400
            raise HTTPException(status_code=400, detail=str(e))
        except RuntimeError as e:
            status_code = 502
            raise HTTPException(status_code=502, detail=str(e))
        except Exception as e:
            status_code = 500
            logger.error(f"Error resolving connect site options: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"Error resolving connect site options: {str(e)}",
            )
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/onboarding/site-options",
                status_code=status_code,
                started_at=started_at,
            )

    @router.post(
        "/connect/onboarding/team-channel-options",
        response_model=ConnectTeamChannelOptionsResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def post_connect_onboarding_team_channel_options_route(
        payload: ConnectTeamChannelOptionsRequest,
        request: Request,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            _log_audit(
                request,
                tenant_context,
                "connect.onboarding.team_channel_options.read",
                tenant_context.username,
            )
            return list_connect_team_channel_options(
                tenant_id=tenant_context.tenant_id,
                azure_tenant_id=payload.azure_tenant_id,
                client_id=payload.client_id,
                client_secret=payload.client_secret,
                team_query=payload.team_query,
                channel_query=payload.channel_query,
                site_id=payload.site_id,
                max_teams=payload.max_teams,
                max_channels_per_team=payload.max_channels_per_team,
            )
        except ValueError as e:
            status_code = 400
            raise HTTPException(status_code=400, detail=str(e))
        except RuntimeError as e:
            status_code = 502
            raise HTTPException(status_code=502, detail=str(e))
        except Exception as e:
            status_code = 500
            logger.error(f"Error resolving connect team channel options: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"Error resolving connect team channel options: {str(e)}",
            )
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/onboarding/team-channel-options",
                status_code=status_code,
                started_at=started_at,
            )

    @router.post(
        "/connect/onboarding",
        response_model=ConnectOnboardingResponse,
        dependencies=[Depends(combined_auth)],
    )
    async def post_connect_onboarding_route(
        payload: ConnectOnboardingRequest,
        request: Request,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        started_at = perf_counter()
        status_code = 200
        try:
            operator = tenant_context.username
            _log_audit(
                request,
                tenant_context,
                "connect.onboarding.create",
                operator,
            )
            # Run in a worker thread: create_connect_onboarding uses time.sleep (reflection poll)
            # and sync boto3; blocking the asyncio loop causes Vite proxy /health socket hang-ups.
            return await asyncio.to_thread(
                create_connect_onboarding,
                tenant_context.tenant_id,
                client_id=payload.client_id,
                site_id=payload.site_id,
                drive_id=payload.drive_id,
                notification_url=payload.notification_url,
                client_secret=payload.client_secret,
                client_state=payload.client_state,
                connection_name=payload.connection_name,
                initialize_subscription=payload.initialize_subscription,
                resource_type=payload.resource_type,
                resource_path=payload.resource_path,
                change_type=payload.change_type,
                target_type=payload.target_type,
                team_id=payload.team_id,
                channel_id=payload.channel_id,
                chat_id=payload.chat_id,
            )
        except ValueError as e:
            status_code = 400
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            status_code = 500
            logger.error(f"Error creating connect onboarding: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error creating connect onboarding: {str(e)}")
        finally:
            _log_connect_observability(
                request,
                tenant_context,
                route="/connect/onboarding",
                status_code=status_code,
                started_at=started_at,
            )

    @router.get("/governance/findings", dependencies=[Depends(combined_auth)])
    async def get_governance_findings(
        limit: int = Query(200, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        statuses: str = Query(
            "new,open,acknowledged,closed",
            description="Comma separated finding status filter",
        ),
        action_required_only: bool = Query(
            False,
            description="Return only actionable findings (status=new/open/acknowledged and medium+ or risk_score>=5.0)",
        ),
        only_active_connect_scopes: bool = Query(
            False,
            description="When true, return only findings whose item_id exists under active Connect drive scopes.",
        ),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Get governance findings from AIReadyGov DynamoDB tables for the caller tenant.
        """
        try:
            _reject_tenant_override(request)
            status_list = [s.strip() for s in statuses.split(",") if s.strip()]
            restrict_item_ids = None
            if only_active_connect_scopes:
                try:
                    drive_ids = get_active_connect_drive_ids(tenant_context.tenant_id)
                    restrict_item_ids = collect_file_metadata_item_ids_for_drives(
                        tenant_context.tenant_id,
                        drive_ids,
                    )
                except ValueError:
                    # Connect が当該テナント向けに無効な場合は全件表示にフォールバックする。
                    restrict_item_ids = None
            _log_audit(
                request,
                tenant_context,
                "governance.findings.read",
                tenant_context.username,
                {
                    "statuses": status_list,
                    "action_required_only": action_required_only,
                    "only_active_connect_scopes": only_active_connect_scopes,
                },
            )
            return list_governance_findings(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
                statuses=status_list if status_list else None,
                action_required_only=action_required_only,
                restrict_item_ids=restrict_item_ids,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting governance findings: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting governance findings: {str(e)}")

    @router.get("/governance/findings/{finding_id}/remediation", dependencies=[Depends(combined_auth)])
    async def get_governance_finding_remediation_route(
        finding_id: str,
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "governance.finding.remediation.read",
                tenant_context.username,
                {"finding_id": finding_id, "target": finding_id},
            )
            log_governance_remediation_route_debug(
                action="get",
                finding_id=finding_id,
                tenant_id=tenant_context.tenant_id,
            )
            return get_governance_finding_remediation(
                tenant_id=tenant_context.tenant_id,
                finding_id=finding_id,
            )
        except GovernanceRemediationProxyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting governance finding remediation: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting governance finding remediation: {str(e)}")

    @router.post("/governance/findings/{finding_id}/remediation/propose", dependencies=[Depends(combined_auth)])
    async def propose_governance_finding_remediation_route(
        finding_id: str,
        force: bool = Query(False, description="Force regenerate remediation proposal"),
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="propose_governance_remediation"
            )
            _log_audit(
                request,
                tenant_context,
                "governance.finding.remediation.propose",
                resolved_operator,
                {"finding_id": finding_id, "target": finding_id, "force": force},
            )
            log_governance_remediation_route_debug(
                action="propose",
                finding_id=finding_id,
                tenant_id=tenant_context.tenant_id,
            )
            return propose_governance_remediation(
                tenant_id=tenant_context.tenant_id,
                finding_id=finding_id,
                operator=resolved_operator,
                force=force,
            )
        except GovernanceRemediationProxyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error proposing governance finding remediation: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error proposing governance finding remediation: {str(e)}")

    @router.post("/governance/findings/{finding_id}/remediation/approve", dependencies=[Depends(combined_auth)])
    async def approve_governance_finding_remediation_route(
        finding_id: str,
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="approve_governance_remediation"
            )
            _log_audit(
                request,
                tenant_context,
                "governance.finding.remediation.approve",
                resolved_operator,
                {"finding_id": finding_id, "target": finding_id},
            )
            log_governance_remediation_route_debug(
                action="approve",
                finding_id=finding_id,
                tenant_id=tenant_context.tenant_id,
            )
            return approve_governance_remediation(
                tenant_id=tenant_context.tenant_id,
                finding_id=finding_id,
                operator=resolved_operator,
            )
        except GovernanceRemediationProxyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error approving governance finding remediation: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error approving governance finding remediation: {str(e)}")

    @router.post("/governance/findings/{finding_id}/remediation/execute", dependencies=[Depends(combined_auth)])
    async def execute_governance_finding_remediation_route(
        finding_id: str,
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="execute_governance_remediation"
            )
            _log_audit(
                request,
                tenant_context,
                "governance.finding.remediation.execute",
                resolved_operator,
                {"finding_id": finding_id, "target": finding_id},
            )
            log_governance_remediation_route_debug(
                action="execute",
                finding_id=finding_id,
                tenant_id=tenant_context.tenant_id,
            )
            return execute_governance_remediation(
                tenant_id=tenant_context.tenant_id,
                finding_id=finding_id,
                operator=resolved_operator,
            )
        except GovernanceRemediationProxyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error executing governance finding remediation: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error executing governance finding remediation: {str(e)}")

    @router.post("/governance/findings/{finding_id}/remediation/rollback", dependencies=[Depends(combined_auth)])
    async def rollback_governance_finding_remediation_route(
        finding_id: str,
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="rollback_governance_remediation"
            )
            _log_audit(
                request,
                tenant_context,
                "governance.finding.remediation.rollback",
                resolved_operator,
                {"finding_id": finding_id, "target": finding_id},
            )
            log_governance_remediation_route_debug(
                action="rollback",
                finding_id=finding_id,
                tenant_id=tenant_context.tenant_id,
            )
            return rollback_governance_remediation(
                tenant_id=tenant_context.tenant_id,
                finding_id=finding_id,
                operator=resolved_operator,
            )
        except GovernanceRemediationProxyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error rolling back governance finding remediation: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error rolling back governance finding remediation: {str(e)}")

    @router.post("/governance/findings/{finding_id}/complete", dependencies=[Depends(combined_auth)])
    async def mark_governance_finding_completed_route(
        finding_id: str,
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="mark_governance_finding_completed"
            )
            _log_audit(
                request,
                tenant_context,
                "governance.finding.complete.mark",
                resolved_operator,
                {"finding_id": finding_id, "target": finding_id},
            )
            return mark_governance_finding_completed(
                tenant_id=tenant_context.tenant_id,
                finding_id=finding_id,
                operator=resolved_operator,
            )
        except GovernanceRemediationProxyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc))
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as e:
            logger.error(f"Error marking governance finding completed: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error marking governance finding completed: {str(e)}")

    @router.post("/governance/findings/{finding_id}/remediation/exception", dependencies=[Depends(combined_auth)])
    async def register_governance_finding_exception_route(
        finding_id: str,
        payload: dict | None = Body(default=None),
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            normalized_payload = payload or {}
            _reject_tenant_override(request, normalized_payload)
            resolved_operator = _resolve_operator(
                tenant_context, operator or normalized_payload.get("operator"), action_name="register_governance_exception"
            )
            _log_audit(
                request,
                tenant_context,
                "governance.finding.exception.register",
                resolved_operator,
                {"finding_id": finding_id, "target": finding_id},
            )
            return register_governance_finding_exception(
                tenant_id=tenant_context.tenant_id,
                finding_id=finding_id,
                operator=resolved_operator,
                exception_type=str(normalized_payload.get("exception_type") or ""),
                duration_days=(
                    int(normalized_payload.get("duration_days"))
                    if normalized_payload.get("duration_days") is not None
                    else None
                ),
                exception_review_due_at=(
                    str(normalized_payload.get("exception_review_due_at")).strip()
                    if normalized_payload.get("exception_review_due_at")
                    else None
                ),
                reason=str(normalized_payload.get("reason")).strip() if normalized_payload.get("reason") else None,
                exception_ticket=(
                    str(normalized_payload.get("exception_ticket")).strip()
                    if normalized_payload.get("exception_ticket")
                    else None
                ),
                scope=normalized_payload.get("scope") if isinstance(normalized_payload.get("scope"), dict) else None,
            )
        except GovernanceRemediationProxyError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc))
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception as e:
            logger.error(f"Error registering governance exception: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error registering governance exception: {str(e)}")

    @router.get("/governance/overview", dependencies=[Depends(combined_auth)])
    async def get_governance_overview_route(
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "governance.overview.read",
                tenant_context.username,
            )
            return get_governance_overview(tenant_id=tenant_context.tenant_id)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting governance overview: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting governance overview: {str(e)}")

    @router.get("/governance/suppressions", dependencies=[Depends(combined_auth)])
    async def get_governance_suppressions_route(
        limit: int = Query(200, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        expiring_within_hours: int = Query(
            24,
            description="Return only suppressions expiring within N hours (0 to disable filter)",
            ge=0,
            le=720,
        ),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "governance.suppressions.read",
                tenant_context.username,
                {"expiring_within_hours": expiring_within_hours},
            )
            return list_governance_suppressions(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
                expiring_within_hours=expiring_within_hours if expiring_within_hours > 0 else None,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting governance suppressions: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting governance suppressions: {str(e)}")

    @router.post("/governance/scans/daily", dependencies=[Depends(combined_auth)])
    async def trigger_governance_scan_route(
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="trigger_governance_daily_scan"
            )
            correlation_id = getattr(request.state, "correlation_id", "")
            _log_audit(
                request,
                tenant_context,
                "governance.scan.daily.trigger",
                resolved_operator,
            )
            return trigger_governance_daily_scan(
                tenant_id=tenant_context.tenant_id,
                operator=resolved_operator,
                correlation_id=correlation_id,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error triggering governance daily scan: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error triggering governance daily scan: {str(e)}")

    @router.get("/governance/policies", dependencies=[Depends(combined_auth)])
    async def get_governance_policies_route(
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "governance.policies.read",
                tenant_context.username,
            )
            return list_governance_policies(tenant_id=tenant_context.tenant_id)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting governance policies: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting governance policies: {str(e)}")

    @router.post("/governance/policies", dependencies=[Depends(combined_auth)])
    async def create_governance_policy_route(
        payload: dict,
        dry_run: bool = Query(False, description="Run policy simulation without persisting"),
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request, payload)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="create_governance_policy"
            )
            correlation_id = getattr(request.state, "correlation_id", "")
            _log_audit(
                request,
                tenant_context,
                "governance.policy.create",
                resolved_operator,
            )
            return create_governance_policy(
                tenant_id=tenant_context.tenant_id,
                payload=payload,
                operator=resolved_operator,
                correlation_id=correlation_id,
                dry_run=dry_run,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating governance policy: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error creating governance policy: {str(e)}")

    @router.put("/governance/policies/{policy_id:path}", dependencies=[Depends(combined_auth)])
    async def update_governance_policy_route(
        policy_id: str,
        payload: dict,
        dry_run: bool = Query(False, description="Run policy simulation without persisting"),
        operator: str = Query("", description="Optional operator override (admin only)"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request, payload)
            resolved_operator = _resolve_operator(
                tenant_context, operator, action_name="update_governance_policy"
            )
            correlation_id = getattr(request.state, "correlation_id", "")
            _log_audit(
                request,
                tenant_context,
                "governance.policy.update",
                resolved_operator,
            )
            return update_governance_policy(
                tenant_id=tenant_context.tenant_id,
                policy_id=policy_id,
                payload=payload,
                operator=resolved_operator,
                correlation_id=correlation_id,
                dry_run=dry_run,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating governance policy: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error updating governance policy: {str(e)}")

    @router.post("/governance/policies/simulate", dependencies=[Depends(combined_auth)])
    async def simulate_governance_policy_route(
        payload: dict,
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request, payload)
            _log_audit(
                request,
                tenant_context,
                "governance.policy.simulate",
                tenant_context.username,
            )
            return simulate_governance_policy(
                tenant_id=tenant_context.tenant_id,
                payload=payload,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error simulating governance policy: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error simulating governance policy: {str(e)}")

    @router.get("/governance/scan-jobs", dependencies=[Depends(combined_auth)])
    async def get_governance_scan_jobs_route(
        limit: int = Query(100, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "governance.scan_jobs.read",
                tenant_context.username,
            )
            return list_governance_scan_jobs(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting governance scan jobs: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting governance scan jobs: {str(e)}")

    @router.get("/governance/audit", dependencies=[Depends(combined_auth)])
    async def get_governance_audit_route(
        limit: int = Query(100, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "governance.audit.read",
                tenant_context.username,
            )
            return list_governance_audit_logs(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting governance audit logs: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error getting governance audit logs: {str(e)}")

    @router.get("/ontology/overview", dependencies=[Depends(combined_auth)])
    async def get_ontology_overview_route(
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "ontology.overview.read",
                tenant_context.username,
            )
            return get_ontology_overview(tenant_id=tenant_context.tenant_id)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting ontology overview: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error getting ontology overview: {str(e)}",
                request=request,
            )

    @router.get("/ontology/unified-metadata", dependencies=[Depends(combined_auth)])
    async def get_ontology_unified_metadata_route(
        limit: int = Query(200, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        only_active_connect_scopes: bool = Query(
            False,
            description="When true, list only rows whose item_id exists under active Connect drive scopes (aligns with governance findings).",
        ),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "ontology.unified_metadata.read",
                tenant_context.username,
                {"only_active_connect_scopes": only_active_connect_scopes},
            )
            return list_ontology_unified_metadata(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
                only_active_connect_scopes=only_active_connect_scopes,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting ontology unified metadata: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error getting ontology unified metadata: {str(e)}",
                request=request,
            )

    @router.post("/ontology/unified-metadata/{item_id}/ai-fill", dependencies=[Depends(combined_auth)])
    async def ai_fill_ontology_profile(
        item_id: str,
        payload: dict | None = Body(default=None),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """Run AI inference to auto-fill owner/project/topic/category for a single item."""
        try:
            _reject_tenant_override(request)
            use_llm_payload = (payload or {}).get("use_llm", None)
            use_llm = bool(use_llm_payload) if isinstance(use_llm_payload, bool) else None
            result = ai_fill_unified_metadata_profile(
                tenant_id=tenant_context.tenant_id,
                item_id=item_id,
                use_llm=use_llm,
            )
            _log_audit(request, tenant_context, "ontology.unified_metadata.ai_fill", tenant_context.username, {"item_id": item_id})
            return result
        except ValueError as e:
            http_status, error_code = _ontology_error_code_from_value_error(
                str(e),
                default_400="ontology.invalid_request",
                default_404="ontology.item_not_found",
            )
            raise _ontology_http_exception(
                status_code=http_status,
                error_code=error_code,
                detail=str(e),
                request=request,
            )
        except Exception as e:
            logger.error(f"Error in AI fill for '{item_id}': {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=str(e),
                request=request,
            )

    @router.put("/ontology/unified-metadata/{item_id}", dependencies=[Depends(combined_auth)])
    async def update_ontology_metadata_profile(
        item_id: str,
        payload: dict = Body(...),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """User-driven manual edit of owner/project/topic/canonical_doc with quality recalc."""
        try:
            _reject_tenant_override(request)
            result = update_unified_metadata_profile(
                tenant_id=tenant_context.tenant_id,
                item_id=item_id,
                owner=payload.get("owner"),
                project=payload.get("project"),
                topic_categories=payload.get("topic_categories"),
                canonical_doc_id=payload.get("canonical_doc_id"),
            )
            _log_audit(request, tenant_context, "ontology.unified_metadata.update", tenant_context.username, {"item_id": item_id})
            return result
        except ValueError as e:
            http_status, error_code = _ontology_error_code_from_value_error(
                str(e),
                default_400="ontology.invalid_request",
                default_404="ontology.item_not_found",
            )
            raise _ontology_http_exception(
                status_code=http_status,
                error_code=error_code,
                detail=str(e),
                request=request,
            )
        except Exception as e:
            logger.error(f"Error updating metadata for '{item_id}': {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=str(e),
                request=request,
            )

    @router.get("/ontology/entity-master", dependencies=[Depends(combined_auth)])
    async def get_ontology_entity_master_route(
        limit: int = Query(200, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "ontology.entity_master.read",
                tenant_context.username,
            )
            return list_ontology_entity_master(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting ontology entity master: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error getting ontology entity master: {str(e)}",
                request=request,
            )

    @router.get("/ontology/audit", dependencies=[Depends(combined_auth)])
    async def get_ontology_audit_route(
        limit: int = Query(200, description="Maximum rows", ge=1, le=500),
        offset: int = Query(0, description="Pagination offset", ge=0),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        try:
            _reject_tenant_override(request)
            _log_audit(
                request,
                tenant_context,
                "ontology.audit.read",
                tenant_context.username,
            )
            return list_ontology_audit_logs(
                tenant_id=tenant_context.tenant_id,
                limit=limit,
                offset=offset,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting ontology audit logs: {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error getting ontology audit logs: {str(e)}",
                request=request,
            )

    @router.post("/ontology/entity-candidates/{candidate_id}/resolve-existing", dependencies=[Depends(combined_auth)])
    async def resolve_ontology_candidate_existing(
        candidate_id: str,
        target_entity_id: str = Query(..., description="Entity dictionary id to merge into"),
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Resolve extracted candidate by linking it to an existing dictionary entity.
        """
        try:
            _reject_tenant_override(request)
            resolved_operator = tenant_context.username
            _log_audit(
                request,
                tenant_context,
                "ontology.candidate.resolve_existing",
                resolved_operator,
                {"candidate_id": candidate_id, "target_entity_id": target_entity_id},
            )
            return resolve_ontology_entity_candidate_existing(
                tenant_id=tenant_context.tenant_id,
                candidate_id=candidate_id,
                target_entity_id=target_entity_id,
                operator=resolved_operator,
            )
        except ValueError as e:
            message = str(e)
            if "dictionary entity not found" in message.lower() or "candidate not found" in message.lower():
                raise _ontology_http_exception(
                    status_code=status.HTTP_404_NOT_FOUND,
                    error_code=(
                        "ontology.candidate_not_found"
                        if "candidate not found" in message.lower()
                        else "ontology.item_not_found"
                    ),
                    detail=message,
                    request=request,
                )
            if "already resolved" in message.lower():
                raise _ontology_http_exception(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    error_code="ontology.invalid_request",
                    detail=message,
                    request=request,
                )
            raise _ontology_http_exception(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code="ontology.target_entity_invalid",
                detail=message,
                request=request,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error resolving ontology candidate(existing): {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error resolving ontology candidate: {str(e)}",
                request=request,
            )

    @router.post("/ontology/entity-candidates/{candidate_id}/register-new", dependencies=[Depends(combined_auth)])
    async def register_ontology_candidate_new(
        candidate_id: str,
        request: Request = None,
        tenant_context: TenantContext = Depends(get_tenant_context_dependency(api_key)),
    ):
        """
        Resolve extracted candidate by registering a new dictionary entity.
        """
        try:
            _reject_tenant_override(request)
            resolved_operator = tenant_context.username
            _log_audit(
                request,
                tenant_context,
                "ontology.candidate.register_new",
                resolved_operator,
                {"candidate_id": candidate_id},
            )
            return register_ontology_entity_candidate_new(
                tenant_id=tenant_context.tenant_id,
                candidate_id=candidate_id,
                operator=resolved_operator,
            )
        except ValueError as e:
            message = str(e)
            if "candidate not found" in message.lower():
                raise _ontology_http_exception(
                    status_code=status.HTTP_404_NOT_FOUND,
                    error_code="ontology.candidate_not_found",
                    detail=message,
                    request=request,
                )
            raise _ontology_http_exception(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code="ontology.invalid_request",
                detail=message,
                request=request,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error resolving ontology candidate(new): {str(e)}")
            logger.error(traceback.format_exc())
            raise _ontology_http_exception(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="ontology.internal_error",
                detail=f"Error resolving ontology candidate: {str(e)}",
                request=request,
            )

    @router.get("/graph/entity/exists", dependencies=[Depends(combined_auth)])
    async def check_entity_exists(
        name: str = Query(..., description="Entity name to check"),
    ):
        """
        Check if an entity with the given name exists in the knowledge graph

        Args:
            name (str): Name of the entity to check

        Returns:
            Dict[str, bool]: Dictionary with 'exists' key indicating if entity exists
        """
        raise _embedded_rag_graph_disabled()

    @router.post("/graph/entity/edit", dependencies=[Depends(combined_auth)])
    async def update_entity(request: EntityUpdateRequest):
        """
        Update an entity's properties in the knowledge graph

        Args:
            request (EntityUpdateRequest): Request containing entity name, updated data, and rename flag

        Returns:
            Dict: Updated entity information
        """
        raise _embedded_rag_graph_disabled()

    @router.post("/graph/relation/edit", dependencies=[Depends(combined_auth)])
    async def update_relation(request: RelationUpdateRequest):
        """Update a relation's properties in the knowledge graph

        Args:
            request (RelationUpdateRequest): Request containing source ID, target ID and updated data

        Returns:
            Dict: Updated relation information
        """
        raise _embedded_rag_graph_disabled()

    return router

"""Feature: source.multi_fetch

First vertical slice of the DatasourceFeature pattern. Owns the per-datasource
`source.multi_fetch` block: schema, defaults, and pre-flight validation that
today is buried inline in DataSourceABCImpl.multi_fetch().
"""

from __future__ import annotations

from typing import Any

from config_features.base import DatasourceFeature, FeatureIssue
from config_features.registry import DatasourceFeatureRegistry
from utils.data_source_config_dto import (
    SourceMultiFetchDTO,
    SourceMultiFetchStrategy,
)


@DatasourceFeatureRegistry.register
class SourceMultiFetchFeature(DatasourceFeature):
    KEY = "source.multi_fetch"
    SCHEMA = SourceMultiFetchDTO
    DESCRIPTION = """
    Controls fan-out HTTP fetching for a single datasource run.

    Strategies:
      - expand_params       Cartesian product over `expand` params, merged with
                            constant `params`. One request per combination.
      - url_template        Format `url_template` once per index of
                            parallel-aligned `template_params` lists.
      - explicit_url_list   Use the literal `urls` list (or read URLs from a
                            file via SourceInputDTO).

    Tunables: fetch_workers, request_timeout, retry_attempts, retry_backoff,
    inter_request_delay, fail_fast.

    Consumed by: DataSourceABCImpl.multi_fetch() during the extract phase.
    Skipped when source.fetch is local or multi_fetch.enable is false.
    """

    @classmethod
    def validate(
        cls, parsed: SourceMultiFetchDTO | None, datasource_name: str
    ) -> list[FeatureIssue]:
        issues: list[FeatureIssue] = []
        if parsed is None or not getattr(parsed, "enable", False):
            return issues

        def err(msg: str) -> None:
            issues.append(FeatureIssue(datasource_name, cls.KEY, msg, "error"))

        def warn(msg: str) -> None:
            issues.append(FeatureIssue(datasource_name, cls.KEY, msg, "warning"))

        strategy = parsed.strategy
        if not SourceMultiFetchStrategy.has_value(strategy):
            err(
                f"invalid strategy '{strategy}'. Must be one of: "
                + ", ".join(s.value for s in SourceMultiFetchStrategy)
            )
            return issues

        if strategy == SourceMultiFetchStrategy.EXPAND_PARAMS.value:
            if not parsed.expand and not parsed.params:
                err("strategy=expand_params requires `expand` or `params`")

        elif strategy == SourceMultiFetchStrategy.URL_TEMPLATE.value:
            if not parsed.url_template:
                err("strategy=url_template requires `url_template`")
            tp = parsed.template_params
            if not tp:
                err("strategy=url_template requires `template_params`")
            elif not isinstance(tp, dict) or not tp:
                err("`template_params` must be a non-empty mapping of name → list")
            else:
                lengths = {k: len(v) for k, v in tp.items() if isinstance(v, list)}
                non_lists = [k for k, v in tp.items() if not isinstance(v, list)]
                if non_lists:
                    err(f"template_params values must be lists; non-list keys: {non_lists}")
                elif len(set(lengths.values())) > 1:
                    err(f"template_params lists must have equal length, got {lengths}")

        elif strategy == SourceMultiFetchStrategy.EXPLICIT_URL_LIST.value:
            if not parsed.urls:
                err("strategy=explicit_url_list requires `urls` (list or SourceInputDTO)")

        # numeric sanity
        for field, minimum in (
            ("fetch_workers", 1),
            ("request_timeout", 1),
            ("retry_attempts", 1),
        ):
            val = getattr(parsed, field, None)
            if val is not None and val < minimum:
                warn(f"{field}={val} is below recommended minimum {minimum}; runtime will clamp")

        return issues

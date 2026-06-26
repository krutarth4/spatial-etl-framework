from dataclasses import dataclass
from typing import Literal


VALID_TRIGGER_NAMES = {"run_once", "date", "interval", "cron", "calendar_interval"}

# Trigger types that MUST have a config block
REQUIRES_CONFIG = {"interval", "cron", "calendar_interval"}

# Trigger types that MUST NOT have a config block
FORBIDS_CONFIG = {"run_once", "date"}

# At least one of these must appear in an interval/calendar_interval config
INTERVAL_TIME_UNITS = {"weeks", "days", "hours", "minutes", "seconds"}
CALENDAR_INTERVAL_UNITS = {"years", "months", "weeks", "days", "hours", "minutes", "seconds"}

# Valid field names for a cron config
CRON_FIELDS = {"year", "month", "day", "week", "day_of_week", "hour", "minute", "second"}

# Keys allowed inside each trigger's config block (unknown keys → warning)
INTERVAL_ALLOWED_KEYS = INTERVAL_TIME_UNITS | {"jitter"}
CRON_ALLOWED_KEYS = CRON_FIELDS | {"start_date", "end_date", "timezone", "jitter"}
CALENDAR_INTERVAL_ALLOWED_KEYS = CALENDAR_INTERVAL_UNITS | {"start", "end", "timezone", "jitter"}


@dataclass
class TriggerIssue:
    datasource_name: str
    message: str
    level: Literal["error", "warning"] = "error"

    def __str__(self) -> str:
        tag = "ERROR" if self.level == "error" else "WARN "
        return f"[{tag}] datasource '{self.datasource_name}': {self.message}"


def _validate_trigger_type(datasource_name: str, trigger_type: dict) -> list[TriggerIssue]:
    issues: list[TriggerIssue] = []
    name = trigger_type.get("name")
    config = trigger_type.get("config")
    start_date = trigger_type.get("start_date")

    # ── name presence and validity ────────────────────────────────────────────
    if not name:
        issues.append(TriggerIssue(
            datasource_name,
            "trigger.type.name is missing. Must be one of: "
            + ", ".join(sorted(VALID_TRIGGER_NAMES)),
        ))
        return issues  # nothing more to check without a name

    if name not in VALID_TRIGGER_NAMES:
        issues.append(TriggerIssue(
            datasource_name,
            f"Unknown trigger name '{name}'. Must be one of: "
            + ", ".join(sorted(VALID_TRIGGER_NAMES)),
        ))
        return issues

    # ── run_once ──────────────────────────────────────────────────────────────
    if name == "run_once":
        if config:
            issues.append(TriggerIssue(
                datasource_name,
                "Trigger 'run_once' does not use 'config' — the block is ignored. Remove it.",
                level="warning",
            ))
        if start_date:
            issues.append(TriggerIssue(
                datasource_name,
                "Trigger 'run_once' ignores 'start_date' — it always runs immediately. "
                "Use trigger 'date' if you want a specific run time.",
                level="warning",
            ))
        return issues

    # ── date ─────────────────────────────────────────────────────────────────
    if name == "date":
        if config:
            issues.append(TriggerIssue(
                datasource_name,
                "Trigger 'date' does not use 'config'. Remove the 'config' block and set "
                "'start_date' instead (that becomes the run_date).",
                level="warning",
            ))
        if not start_date:
            issues.append(TriggerIssue(
                datasource_name,
                "Trigger 'date' has no 'start_date'. Without it the job runs at the current "
                "time (effectively immediate). Add 'start_date: <ISO datetime>' to schedule it.",
                level="warning",
            ))
        return issues

    # ── triggers that require a config block ──────────────────────────────────
    if name in REQUIRES_CONFIG:
        if not config or not isinstance(config, dict):
            issues.append(TriggerIssue(
                datasource_name,
                f"Trigger '{name}' requires a 'config' block with at least one field. "
                + _config_hint(name),
            ))
            return issues  # nothing inside to validate

        # Per-trigger config key validation
        if name == "interval":
            _check_interval_config(datasource_name, config, issues)
        elif name == "cron":
            _check_cron_config(datasource_name, config, issues)
        elif name == "calendar_interval":
            _check_calendar_interval_config(datasource_name, config, issues)

    return issues


def _check_interval_config(datasource_name: str, config: dict, issues: list[TriggerIssue]) -> None:
    present_units = set(config.keys()) & INTERVAL_TIME_UNITS
    if not present_units:
        issues.append(TriggerIssue(
            datasource_name,
            "Trigger 'interval' config must include at least one time unit: "
            + ", ".join(sorted(INTERVAL_TIME_UNITS))
            + ". Example:  config: {hours: 6}",
        ))
    else:
        for unit in present_units:
            val = config[unit]
            if not isinstance(val, (int, float)) or val <= 0:
                issues.append(TriggerIssue(
                    datasource_name,
                    f"Trigger 'interval' config field '{unit}' must be a positive number, got: {val!r}",
                ))

    unknown = set(config.keys()) - INTERVAL_ALLOWED_KEYS
    if unknown:
        issues.append(TriggerIssue(
            datasource_name,
            f"Trigger 'interval' config has unrecognised keys: {', '.join(sorted(unknown))}. "
            "Valid keys: " + ", ".join(sorted(INTERVAL_ALLOWED_KEYS)),
            level="warning",
        ))


def _check_cron_config(datasource_name: str, config: dict, issues: list[TriggerIssue]) -> None:
    present_fields = set(config.keys()) & CRON_FIELDS
    if not present_fields:
        issues.append(TriggerIssue(
            datasource_name,
            "Trigger 'cron' config should include at least one cron field: "
            + ", ".join(sorted(CRON_FIELDS))
            + ". Without any field it fires every second.",
            level="warning",
        ))

    unknown = set(config.keys()) - CRON_ALLOWED_KEYS
    if unknown:
        issues.append(TriggerIssue(
            datasource_name,
            f"Trigger 'cron' config has unrecognised keys: {', '.join(sorted(unknown))}. "
            "Valid keys: " + ", ".join(sorted(CRON_ALLOWED_KEYS)),
            level="warning",
        ))


def _check_calendar_interval_config(datasource_name: str, config: dict, issues: list[TriggerIssue]) -> None:
    present_units = set(config.keys()) & CALENDAR_INTERVAL_UNITS
    if not present_units:
        issues.append(TriggerIssue(
            datasource_name,
            "Trigger 'calendar_interval' config must include at least one time unit: "
            + ", ".join(sorted(CALENDAR_INTERVAL_UNITS))
            + ". Example:  config: {weeks: 2}",
        ))
    else:
        for unit in present_units:
            val = config[unit]
            if not isinstance(val, (int, float)) or val <= 0:
                issues.append(TriggerIssue(
                    datasource_name,
                    f"Trigger 'calendar_interval' config field '{unit}' must be a positive number, got: {val!r}",
                ))

    unknown = set(config.keys()) - CALENDAR_INTERVAL_ALLOWED_KEYS
    if unknown:
        issues.append(TriggerIssue(
            datasource_name,
            f"Trigger 'calendar_interval' config has unrecognised keys: {', '.join(sorted(unknown))}. "
            "Valid keys: " + ", ".join(sorted(CALENDAR_INTERVAL_ALLOWED_KEYS)),
            level="warning",
        ))


def _config_hint(trigger_name: str) -> str:
    hints = {
        "interval": "Example:  trigger: {type: {name: interval, config: {hours: 6}}}",
        "cron": "Example:  trigger: {type: {name: cron, config: {hour: 3, minute: 0}}}",
        "calendar_interval": "Example:  trigger: {type: {name: calendar_interval, config: {weeks: 1}}}",
    }
    return hints.get(trigger_name, "")


def validate_all_job_triggers(datasources: list[dict]) -> tuple[list[TriggerIssue], list[TriggerIssue]]:
    """
    Validate job.trigger configs for every datasource.

    Skips datasources that have no 'job' section.
    Validates disabled datasources too — misconfigured entries should be caught early.

    Returns (errors, warnings).
    """
    errors: list[TriggerIssue] = []
    warnings: list[TriggerIssue] = []

    for ds in datasources:
        if not isinstance(ds, dict):
            continue

        name = ds.get("name", "<unnamed>")
        job = ds.get("job")
        if not job or not isinstance(job, dict):
            continue  # no job section — scheduler not used for this datasource

        trigger = job.get("trigger")
        if not trigger or not isinstance(trigger, dict):
            errors.append(TriggerIssue(name, "job.trigger block is missing or empty"))
            continue

        trigger_type = trigger.get("type")
        if not trigger_type or not isinstance(trigger_type, dict):
            errors.append(TriggerIssue(name, "job.trigger.type block is missing or empty"))
            continue

        for issue in _validate_trigger_type(name, trigger_type):
            (errors if issue.level == "error" else warnings).append(issue)

    return errors, warnings

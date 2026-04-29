"""Remote command execution with variable substitution."""

from __future__ import annotations

import logging
import re

import click

from delta.connection import Connection
from delta.exceptions import RemoteCommandError, ValidationError
from delta.models import CommandSpec, VariableSpec

logger = logging.getLogger("delta")


def validate_variables(
    specs: list[VariableSpec],
    provided: dict[str, str],
) -> dict[str, str]:
    """Validate provided variables against specs. Returns resolved variable map.

    Raises ValidationError if required variables are missing.
    """
    resolved: dict[str, str] = {}
    missing: list[str] = []

    for spec in specs:
        if spec.name in provided:
            resolved[spec.name] = provided[spec.name]
        elif not spec.required and spec.default is not None:
            resolved[spec.name] = spec.default
        elif not spec.required:
            resolved[spec.name] = ""
        else:
            missing.append(spec.name)

    if missing:
        raise ValidationError(
            f"Missing required variables: {', '.join(missing)}. "
            f"Pass them with --var {missing[0]}=VALUE"
        )
    return resolved


def substitute_variables(cmd: str, variables: dict[str, str]) -> str:
    """Replace ${VAR_NAME} placeholders in a command string.

    Use $${VAR} to get literal ${VAR} in output (bash variable escape).
    """
    # First, protect escaped $${ by replacing with a placeholder
    ESCAPE_MARKER = "\x00DOLLAR\x00"
    cmd = cmd.replace("$${", ESCAPE_MARKER)

    def replacer(match: re.Match) -> str:
        name = match.group(1)
        if name in variables:
            return variables[name]
        return match.group(0)  # Leave unresolved as-is

    result = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", replacer, cmd)

    # Restore escaped ${ 
    result = result.replace(ESCAPE_MARKER, "${")
    return result


def check_undefined_variables(
    commands: list[CommandSpec],
    defined: set[str],
) -> list[str]:
    """Find ${VAR} in commands that have no definition. Returns undefined names."""
    undefined: list[str] = []
    for cmd_spec in commands:
        text = cmd_spec.cmd.replace("$${", "")  # Skip escaped
        for match in re.finditer(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", text):
            name = match.group(1)
            if name not in defined and name not in undefined:
                undefined.append(name)
    return undefined


def execute_commands(
    conn: Connection,
    commands: list[CommandSpec],
    variables: dict[str, str],
    *,
    phase: str = "",
) -> dict[str, str]:
    """Execute remote commands with real-time output streaming.

    Raises RemoteCommandError if a non-optional command fails.
    """
    from delta import ui

    outputs: dict[str, str] = {}
    if not commands:
        return outputs

    phase_label = phase.upper().replace("-", " ").replace("_", " ")
    ui.print_phase(f"COMMANDS ({phase_label})" if phase else "COMMANDS")
    collector = ui.get_collector()

    for i, cmd_spec in enumerate(commands, 1):
        # Skip run_once commands if output already recorded
        if cmd_spec.run_once and cmd_spec.output_key and cmd_spec.output_key in variables:
            ui.print_dim(f"    [{i}/{len(commands)}] skipped (run_once, {cmd_spec.output_key} already set)")
            continue

        resolved_cmd = substitute_variables(cmd_spec.cmd, variables)
        opt_tag = click.style(" [optional]", dim=True) if cmd_spec.optional else ""
        click.echo(click.style(f"    [{i}/{len(commands)}] ", fg="cyan") + resolved_cmd + opt_tag)

        # Stream stdout and stderr in real-time
        stdout, stderr, exit_code = conn.exec_stream(
            resolved_cmd,
            line_callback=lambda line: ui.print_cmd_output(line),
            stderr_callback=lambda line: ui.print_cmd_stderr(line),
        )

        if exit_code != 0:
            if not cmd_spec.optional:
                msg = f"Command failed (exit {exit_code}): {resolved_cmd}"
                if collector:
                    collector.error(phase or "commands", msg)
                raise RemoteCommandError(msg)
            # Optional — note inline + add to summary
            ui.print_dim(f"(exited with code {exit_code})")
            if collector:
                collector.warning(phase or "commands",
                                  f"Optional command exited with {exit_code}: {resolved_cmd}")

        if cmd_spec.save_output and cmd_spec.output_key:
            outputs[cmd_spec.output_key] = stdout.strip()
            ui.print_dim(f"→ saved as '{cmd_spec.output_key}'")

    ui.print_success(f"{len(commands)} command(s) completed.")
    return outputs


def parse_var_args(var_args: tuple[str, ...]) -> dict[str, str]:
    """Parse --var KEY=VALUE arguments into a dict."""
    result: dict[str, str] = {}
    for arg in var_args:
        if "=" not in arg:
            raise ValidationError(f"Invalid variable format: '{arg}'. Expected KEY=VALUE.")
        key, _, value = arg.partition("=")
        key = key.strip()
        if not key:
            raise ValidationError(f"Empty variable name in: '{arg}'.")
        result[key] = value
    return result


def resolve_description(
    template: str,
    variables: dict[str, str] | None = None,
    command_outputs: dict[str, str] | None = None,
    metadata_fields: dict[str, str] | None = None,
) -> str:
    """Resolve ${VAR} placeholders in a description template.

    Sources (last wins):
      1. command_outputs  (save_output results from commands)
      2. variables        (--var KEY=VALUE — highest priority)

    metadata_fields is accepted for backwards compatibility but ignored.
    """
    if not template or "${" not in template:
        return template

    merged: dict[str, str] = {}
    if command_outputs:
        merged.update(command_outputs)
    if variables:
        merged.update(variables)  # --var wins over command outputs

    return substitute_variables(template, merged)

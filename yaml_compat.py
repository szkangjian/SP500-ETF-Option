from __future__ import annotations

import os
import re
from typing import Any


_FORCE_SIMPLE_YAML = os.environ.get("FORCE_SIMPLE_YAML") == "1"

try:
    if _FORCE_SIMPLE_YAML:
        raise ModuleNotFoundError("FORCE_SIMPLE_YAML=1")
    import yaml as _pyyaml  # type: ignore[import-not-found]
except ModuleNotFoundError:
    _pyyaml = None


_SIMPLE_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_.\-/]+$")


def _strip_comments(text: str) -> list[tuple[int, str]]:
    parsed_lines: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        if not raw_line.strip():
            continue
        stripped = raw_line.lstrip()
        if stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(stripped)
        parsed_lines.append((indent, stripped.rstrip()))
    return parsed_lines


def _parse_quoted_scalar(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] == '"':
        inner = value[1:-1]
        return (
            inner.replace(r"\\", "\\")
            .replace(r"\"", '"')
            .replace(r"\n", "\n")
            .replace(r"\t", "\t")
        )
    if len(value) >= 2 and value[0] == value[-1] == "'":
        inner = value[1:-1]
        return inner.replace("''", "'")
    return value


def _split_inline_items(text: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    quote: str | None = None
    depth = 0
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in {'"', "'"}:
            quote = char
            current.append(char)
            continue
        if char in "[{":
            depth += 1
            current.append(char)
            continue
        if char in "]}":
            depth -= 1
            current.append(char)
            continue
        if char == "," and depth == 0:
            items.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def _parse_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"null", "~"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return _parse_quoted_scalar(value)
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(item) for item in _split_inline_items(inner)]
    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        if not inner:
            return {}
        result: dict[str, Any] = {}
        for item in _split_inline_items(inner):
            key, _, rest = item.partition(":")
            if not _:
                raise ValueError(f"Invalid inline mapping item: {item}")
            result[key.strip()] = _parse_scalar(rest.strip())
        return result
    try:
        if value.startswith("0") and value not in {"0", "0.0"} and not value.startswith("0."):
            raise ValueError
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _parse_block(lines: list[tuple[int, str]], index: int, indent: int) -> tuple[Any, int]:
    if index >= len(lines):
        return {}, index

    current_indent, current_text = lines[index]
    if current_indent < indent:
        return {}, index

    if current_text.startswith("- "):
        sequence: list[Any] = []
        while index < len(lines):
            line_indent, line_text = lines[index]
            if line_indent < indent:
                break
            if line_indent != indent or not line_text.startswith("- "):
                raise ValueError(f"Invalid sequence indentation near: {line_text}")
            item_text = line_text[2:].strip()
            index += 1
            if item_text:
                sequence.append(_parse_scalar(item_text))
            else:
                nested, index = _parse_block(lines, index, indent + 2)
                sequence.append(nested)
        return sequence, index

    mapping: dict[str, Any] = {}
    while index < len(lines):
        line_indent, line_text = lines[index]
        if line_indent < indent:
            break
        if line_indent != indent:
            raise ValueError(f"Unexpected indentation near: {line_text}")
        key, sep, rest = line_text.partition(":")
        if not sep:
            raise ValueError(f"Invalid mapping entry: {line_text}")
        key = key.strip()
        rest = rest.strip()
        index += 1
        if rest:
            mapping[key] = _parse_scalar(rest)
            continue
        if index < len(lines) and lines[index][0] > line_indent:
            nested, index = _parse_block(lines, index, lines[index][0])
            mapping[key] = nested
        else:
            mapping[key] = {}
    return mapping, index


def _simple_safe_load(stream: Any) -> Any:
    if hasattr(stream, "read"):
        text = stream.read()
    else:
        text = str(stream)
    lines = _strip_comments(text)
    if not lines:
        return None
    parsed, index = _parse_block(lines, 0, lines[0][0])
    if index != len(lines):
        raise ValueError("Did not consume the full YAML document.")
    return parsed


def _format_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str) and _SIMPLE_KEY_PATTERN.match(value):
        if value.lower() in {"null", "true", "false"}:
            return f'"{value}"'
        return value
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _simple_dump_lines(value: Any, indent: int = 0) -> list[str]:
    prefix = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if isinstance(item, dict):
                lines.append(f"{prefix}{key}:")
                lines.extend(_simple_dump_lines(item, indent + 2))
            elif isinstance(item, list):
                inline = ", ".join(_format_scalar(element) for element in item)
                lines.append(f"{prefix}{key}: [{inline}]")
            else:
                lines.append(f"{prefix}{key}: {_format_scalar(item)}")
        return lines
    if isinstance(value, list):
        return [f"{prefix}- {_format_scalar(item)}" for item in value]
    return [f"{prefix}{_format_scalar(value)}"]


def _simple_safe_dump(data: Any, sort_keys: bool = False, allow_unicode: bool = True) -> str:
    del allow_unicode
    if sort_keys and isinstance(data, dict):
        data = dict(sorted(data.items(), key=lambda item: item[0]))
    return "\n".join(_simple_dump_lines(data)) + "\n"


def safe_load(stream: Any) -> Any:
    if _pyyaml is not None:
        return _pyyaml.safe_load(stream)
    return _simple_safe_load(stream)


def safe_dump(data: Any, sort_keys: bool = False, allow_unicode: bool = True) -> str:
    if _pyyaml is not None:
        return _pyyaml.safe_dump(data, sort_keys=sort_keys, allow_unicode=allow_unicode)
    return _simple_safe_dump(data, sort_keys=sort_keys, allow_unicode=allow_unicode)

import hashlib
from datetime import date, datetime
from typing import Any

import yaml


class MappingEngine:
    def __init__(self, mapping_yaml: str):
        self.mapping = yaml.safe_load(mapping_yaml)

    def transform_row(self, row: Any) -> dict[str, Any]:
        """
        Transforms a Spark Row into a Meta CAPI event dictionary based on the YAML mapping.
        """
        event = {}
        for field, rule in self.mapping.items():
            # Skip special keys if we add metadata later
            if field in ['version', 'meta']:
                continue

            value = self._apply_rule(rule, row)
            if value is not None:
                event[field] = value
        return event

    def _apply_rule(self, rule: Any, row: Any) -> Any:
        # Handle simple string mapping (shorthand for column source)
        if isinstance(rule, str):
            return rule

        if isinstance(rule, dict):
            # Explicit Literal
            if rule.get('type') == 'literal':
                return rule.get('value')

            # Column Reference
            if 'source' in rule:
                col_name = rule['source']
                val = getattr(row, col_name)

                # Apply transforms
                if 'transform' in rule:
                    transforms = rule['transform']
                    if isinstance(transforms, str):
                        transforms = [transforms]

                    for t in transforms:
                        val = self._apply_transform(t, val)

                return val

            # Nested Object (e.g., user_data, custom_data)
            nested_obj = {}
            for k, v in rule.items():
                val = self._apply_rule(v, row)
                if val is not None:
                    nested_obj[k] = val
            return nested_obj if nested_obj else None

        return rule

    def _apply_transform(self, transform_name: str, value: Any) -> Any:
        if value is None:
            return None

        if transform_name == 'sha256':
            if not isinstance(value, str):
                value = str(value)
            return hashlib.sha256(value.encode('utf-8')).hexdigest()

        elif transform_name == 'normalize':
            if isinstance(value, str):
                return value.strip().lower()
            return value

        elif transform_name == 'normalize_email':
            if isinstance(value, str):
                return value.strip().lower()
            return value

        elif transform_name == 'normalize_phone':
            # Remove symbols, keep numbers
            if isinstance(value, str):
                return ''.join(filter(str.isdigit, value))
            return value

        elif transform_name == 'to_epoch':
            if isinstance(value, (datetime, date)):
                if hasattr(value, 'timestamp'):
                    return int(value.timestamp())
                # For date objects
                return int(datetime.combine(value, datetime.min.time()).timestamp())
            try:
                # Try parsing ISO string?
                # Simple fallback: return as is if int/float
                return int(value)
            except Exception:
                return value

        elif transform_name == 'cast_int':
            try:
                return int(value)
            except Exception:
                return value

        elif transform_name == 'cast_float':
            try:
                return float(value)
            except Exception:
                return value

        elif transform_name == 'cast_string':
            return str(value)

        return value


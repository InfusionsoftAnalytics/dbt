from dbt.api.object import APIObject
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.contracts.graph.parsed

# dbt_identifier = '^[a-zA-Z0-9_]+$'
dbt_identifier = '^.+$'

RELATION_SPEC_CONTRACT = {
    "type": "object",
    'patternProperties': {
        dbt_identifier: {
            "type": "object",
            "properties": {
                "metadata": {
                    "type": "object",
                    "properties": {
                        "schema": {
                            "type": "string"
                        },
                        "name": {
                            "type": "string"
                        },
                        "type": {
                            "type": "string"
                        },
                        "comment": {
                            "type": ["null", "string"]
                        }
                    }
                },
                "columns": {
                    "type": "array",
                    'items': {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string"
                            },
                            "index": {
                                "type": "integer"
                            },
                            "type": {
                                "type": "string"
                            },
                            "comment": {
                                "type": ["null", "string"]
                            },
                            "tests": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "test_name": {
                                            "type": "string"
                                        },
                                        "args": {
                                            "type": "object"
                                        },
                                        "namespace": {
                                            "type": ["null", "string"]
                                        },
                                        "model_name": {
                                            "type": "string"
                                        },
                                    },
                                    "required": ["test_name", "args", "namespace", "model_name"]
                                }
                            },
                            "required": ["name", "index", "type", "comment", "tests"],
                            "additionalProperties": False
                        }
                    }
                },
                "required": ["columns", "metadata"],
                "additionalProperties": False
            }
        },
        "additionalProperties": False
    },
    "additionalProperties": False,
}

CATALOG_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        dbt_identifier: RELATION_SPEC_CONTRACT
    },
    'required': [], # TODO
}

class Catalog(APIObject):
    SCHEMA = CATALOG_CONTRACT

class RelationSpec(APIObject):
    SCHEMA = RELATION_SPEC_CONTRACT

    def to_dict(self):
        return self.serialize()

from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_runners import CompileRunner
from dbt.node_types import NodeType
import dbt.ui.printer

from dbt.task.base_task import RunnableTask

import os, json
from dbt.clients.system import write_file


class CompileTask(RunnableTask):
    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args
        )

        query = {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.executable(),
            "tags": []
        }

        results = runner.run(query, CompileRunner)
        compiled = dict([(r.node['unique_id'], r.node['injected_sql']) for r in results])

        compiled_path = os.path.join(self.project['target-path'], 'compiled.json')
        write_file(compiled_path, json.dumps(compiled))

        dbt.ui.printer.print_timestamped_line('Done.')

        return results

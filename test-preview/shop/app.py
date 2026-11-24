"""The shop's app: the root of the module chain. Workers and the CLI import it."""

from syntasks import SynTasks

from . import live, marketing, orders

# Every task, schedule and channel needs timeout, retries and max_tasks;
# these are the defaults the modules start from.
app = SynTasks(timeout="1m", retries=0, max_tasks=None)
app.queue("default", priorities=2)
app.extends(orders.module, marketing.module, live.module)

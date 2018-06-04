
import agate

agate_null_values = ['', 'na', 'n/a', 'null']

DEFAULT_TYPE_TESTER = agate.TypeTester(types=[
    agate.data_types.Number(null_values=agate_null_values),
    agate.data_types.Date(null_values=agate_null_values),
    agate.data_types.DateTime(null_values=agate_null_values),
    agate.data_types.Boolean(null_values=agate_null_values),
    agate.data_types.Text(cast_nulls=False),
])


def table_from_data(data):
    "Convert list of dictionaries into an Agate table"

    return agate.Table.from_object(data, column_types=DEFAULT_TYPE_TESTER)


def empty_table():
    "Returns an empty Agate table. To be used in place of None"

    return agate.Table(rows=[])


def as_matrix(table):
    "Return an agate table as a matrix of data sans columns"

    return [r.values() for r in table.rows.values()]


def from_csv(abspath):
    return agate.Table.from_csv(abspath, column_types=DEFAULT_TYPE_TESTER)

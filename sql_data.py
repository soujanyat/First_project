####################################################################################################################
## This patch is required with SQLALCHEMYDA
##
## This file defines "query" function of SQLAlchemyDA. Also, adds a new function "query_fullqs" to the DA.
##
## The "query" method patch is required to do following changes in query result,
## 1) Return Zope DateTime object (SQLAlchecmyDA returns python datetime object in the resultset)
## 2) Return Numeric type of object as INT by doing int() conversion (SQLAlchecmyDA returns Numeric type as Decimal() object)
## 3) Return Float type of object as float by doing float() conversion
## 4) Return None type of object as empty string("")
## 5) Return Blob data instead of BLOB object in case of BLOB, CLOB (by calling read() method on these objects)
##
## The new method "query_fullqs" is added to handle insert data and return recent identity value.
## "query" method executes each sql statement separately. In the case of insert query, we have SELECT SCOPE_IDENTITY() call as
## second statement which has to be executed together to get the scope identity value from last insert.
## To do this, we have added new method and any insert statement with SCOPE_IDENTITY call is passed to query_fullqs
####################################################################################################################


import logging
import sys
import time
from DateTime import DateTime

from Products.SQLAlchemyDA.da import SAWrapper
#from dbkit.mssql_hints import *

CONVERSION_TIMEZONE = 'UTC'

LOG = logging.getLogger('SQLAlchemyDA')
types_mapping = {
    'DATE': 'd',
    'IME': 'd',
    'DATETIME': 'd',
    'STRING': 's',
    'LONGINTEGER': 'i',
    'INTEGER': 'i',
    'NUMBER': 'n',
    'BOOLEAN': 'n',
    'ROWID': 'i',
    'BINARY': None,  # ????
}


def query(self, query_string, max_rows=None, query_data=None):
    """ *The* query() method as used by the internal ZSQL
        machinery.
    """
    c = self._wrapper.connection
    cursor = c.cursor()
    rows = []
    desc = None
    nselects = 0
    ts_start = time.time()
    if not query_string.strip():
        return [(), ()]
    for qs in [x for x in query_string.split('\0') if x]:
        qs = str(qs)
        if not qs.strip():
            continue
        LOG.debug(qs)
        if query_data:
            cursor.executedirect(qs, query_data)
            query_data = None
        else:
            if sys.platform == 'linux2':
                cursor.executedirect(qs)
            else:
                #try:
                cursor.executedirect(qs)
                #writeToLog(qs)
                #except:
                #    print qs

        description = cursor.description
        if description is not None:
            nselects += 1

            if nselects > 1:
                raise ValueError("Can't execute multiple SELECTs within a single query")

            if not rows:
                rows = cursor.fetchall()

            desc = description

    LOG.debug('Execution time: %3.3f seconds' % (time.time() - ts_start))

    if desc is None:
        return [], []

    items = []
    has_datetime = False
    has_lob = False
    has_decimal = False
    for  name, type_code, width, internal_size, precision, scale, null_ok in desc:
        if type_code == 2:
            has_decimal = True
            if scale > 0:
                type_cd = 'f'
            else:
                type_cd = 'i'
        elif type_code in [4, -7]:
            has_decimal = True
            type_cd = 'i'
        elif type_code == 93:
            has_datetime = True
            type_cd = 'd'
        else:
            type_cd = 's'

        items.append({'name': name,
                      'type': type_cd,  # types_mapping.get(types_map.get(type_code, None), 's'),
                      'null': null_ok,
                      'width': width,
                      })
    if has_lob or has_decimal:
        result = self._handle_lob_data(items, rows)
    elif has_datetime:
        result = self._munge_datetime_results(items, rows)
    else:
        result = self._null_as_string(items, rows)
    return items, result


def writeToLog(qs):
    """ temp method to write to a file """
    LOG.debug(qs)


def query_fullqs(self, query_string, max_rows=None, query_data=None):
    """ *The* query() method as used by the internal ZSQL
        machinery.
    """

    c = self._wrapper.connection
    cursor = c.cursor()
    rows = []
    description = None
    desc = None
    #nselects = 0
    select_performed = False
    ts_start = time.time()
    if not query_string.strip():
        return [(), ()]
    for qs in [query_string.replace('\0', ';')]:
        qs = str(qs)
        if not qs.strip():
            continue
        LOG.debug(qs)
        if query_data:
            cursor.executedirect(qs, query_data)
            query_data = None
        else:
            cursor.executedirect(qs)

        # Handling mulitple query sets
        if "win" in sys.platform:
            while cursor.nextset():
                description = cursor.description
                if description:
                    rows = cursor.fetchall()
                    desc = description

        else:
            description = cursor.description
            if description is not None:
                if select_performed:
                    raise ValueError("Can't execute multiple SELECTs within a single query")

                select_performed = True
                rows = cursor.fetchall()
                desc = description

    LOG.debug('Execution time: %3.3f seconds' % (time.time() - ts_start))

    if desc is None:
        return [], []

    items = []
    has_datetime = False
    has_lob = False
    has_decimal = False
    for  name, type_code, width, internal_size, precision, scale, null_ok in desc:
        if type_code == 2:
            has_decimal = True
            if scale > 0:
                type_cd = 'f'
            else:
                type_cd = 'i'
        elif type_code == 4:
            has_decimal = True
            type_cd = 'i'
        elif type_code == 93:
            has_datetime = True
            type_cd = 'd'
        else:
            type_cd = 's'

        items.append({'name': name,
                      'type': type_cd,  # types_mapping.get(types_map.get(type_code, None), 's'),
                      'null': null_ok,
                      'width': width,
                      })
    if has_lob or has_decimal:
        result = self._handle_lob_data(items, rows)
    elif has_datetime:
        result = self._munge_datetime_results(items, rows)
    else:
        result = self._null_as_string(items, rows)
    return items, result


class NulledString(str):
    pass


def _check_null(self, dt, val):
    if dt and val:
        try:
            return str(val)
        except:
            return val
    elif val is None:
        return NulledString('')
    return val


def _null_as_string(self, items, results):
    if not results:
        return results
    dtmap = [i['type'] == 's' for i in items]
    ns = []
    for row in results:
        r = tuple([self._check_null(*r) for r in zip(dtmap, row)])
        #r = tuple([[v, ''][v is None] for v in row])
        ns.append(r)
    return ns


def _read_lob(self, dt, val):
    if dt[0] and val:
        x = val.timetuple()[:6]  # + (CONVERSION_TIMEZONE,)
        return DateTime(*x)
    elif dt[1] and val:
        try:
            return val.read()
        except:
            return ''
    elif dt[2] and val:
        return float(str(val))
    elif dt[3] and val:
        try:
            return str(val)
        except:
            return val
    elif val is None:
        return NulledString('')
    elif dt[4] and val is not None:
        return int(val)
    return val


def _handle_lob_data(self, items, results):
    if not results or not items:
        return results
    dtmap = [[i['type'] == 'd', i['type'] == 'l', i['type'] == 'f', i['type'] == 's', i['type'] == 'i'] for i in items]
    nl = []
    for row in results:
        r = tuple([self._read_lob(*r) for r in zip(dtmap, row)])
        nl.append(r)
    return nl


def _datetime_convert(self, dt, val):
    if dt[0] and val:
        # Currently we don't do timezones. Everything is UTC.
        # Ideally we'd get the current Oracle timezone and use that.
        x = val.timetuple()[:6]  # + (CONVERSION_TIMEZONE,)
        return DateTime(*x)
    elif dt[1] and val:
        try:
            return str(val)
        except:
            return val
    return val or ''


def _munge_datetime_results(self, items, results):
    if not results or not items:
        return results
    dtmap = [[i['type'] == 'd', i['type'] == 's'] for i in items]
    nr = []
    for row in results:
        r = tuple([self._datetime_convert(*r) for r in zip(dtmap, row)])
        nr.append(r)
    return nr


def manage_start(self, RESPONSE=None):
    """ start engine """
    try:
        #self.query('COMMIT');
        if RESPONSE:
            msg = 'Database connection opened'
            RESPONSE.redirect(self.absolute_url() + '/manage_workspace?manage_tabs_message=%s' % msg)
    except Exception, e:
        if RESPONSE:
            msg = 'Database connection could not be opened (%s)' % e
            RESPONSE.redirect(self.absolute_url() + '/manage_workspace?manage_tabs_message=%s' % msg)
        else:
            raise

SAWrapper.query = query
SAWrapper._read_lob = _read_lob
SAWrapper._handle_lob_data = _handle_lob_data
SAWrapper._datetime_convert = _datetime_convert
SAWrapper._null_as_string = _null_as_string
SAWrapper._munge_datetime_results = _munge_datetime_results
SAWrapper._check_null = _check_null
SAWrapper.query_fullqs = query_fullqs
SAWrapper.manage_start = manage_start


print "In SQLAlchemyDA, QUERY method is patched"
